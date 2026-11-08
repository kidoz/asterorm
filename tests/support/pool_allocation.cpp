#include "asterorm/pool/connection_pool.hpp"
#include <cstdlib>
#include <new>

namespace {
thread_local bool deny_allocation{};
thread_local std::size_t allocations{};
} // namespace
void* operator new(std::size_t size) {
    ++allocations;
    if (deny_allocation) {
        throw std::bad_alloc{};
    }
    if (void* memory = std::malloc(size ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc{};
}
void operator delete(void* memory) noexcept {
    std::free(memory);
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void operator delete[](void* memory) noexcept {
    ::operator delete(memory);
}
void operator delete(void* memory, [[maybe_unused]] std::size_t size) noexcept {
    ::operator delete(memory);
}
void operator delete[](void* memory, [[maybe_unused]] std::size_t size) noexcept {
    ::operator delete(memory);
}

namespace {
struct connection {
    int* closes;
    explicit connection(int& observed) noexcept : closes(&observed) {}
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;
    connection(connection&& other) noexcept : closes(std::exchange(other.closes, nullptr)) {}
    ~connection() noexcept {
        close();
    }
    [[nodiscard]] bool is_open() const noexcept {
        return closes != nullptr;
    }
    void close() noexcept {
        if (closes) {
            ++*std::exchange(closes, nullptr);
        }
    }
};
struct driver {
    int* closes;
    [[nodiscard]] asterorm::result<connection>
    connect([[maybe_unused]] const std::string& settings) const {
        return connection{*closes};
    }
};
int allocation_checks() {
    int closes{};
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    using pool_type = asterorm::connection_pool<driver>;
    pool_type pool{driver{&closes}, config};
    deny_allocation = true;
    bool failed{};
    try {
        auto unexpected = pool.acquire();
    } catch (const std::bad_alloc&) {
        failed = true;
    }
    deny_allocation = false;
    if (!failed || closes != 1 || pool.stats().total != 0) {
        return 1;
    }
    auto acquired = pool.acquire();
    if (!acquired) {
        return 2;
    }
    const auto before = allocations;
    deny_allocation = true;
    acquired->release_to_pool();
    pool.close();
    deny_allocation = false;
    if (allocations != before || closes != 2 || pool.stats().total != 0) {
        return 3;
    }
    auto facade = std::make_unique<pool_type>(driver{&closes}, config);
    auto retained = facade->acquire();
    if (!retained) {
        return 4;
    }
    const auto late_before = allocations;
    deny_allocation = true;
    facade.reset();
    retained->release_to_pool();
    deny_allocation = false;
    if (allocations != late_before || closes != 3) {
        return 5;
    }
    config.leak_threshold = std::chrono::milliseconds{1};
    config.maintenance_interval = std::chrono::hours{1};
    pool_type diagnosed{driver{&closes}, config};
    auto overdue = diagnosed.acquire();
    if (!overdue) {
        return 6;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{2});
    const auto diagnostic_before = allocations;
    deny_allocation = true;
    overdue->release_to_pool(); // Enqueue the warning without allocating.
    diagnosed.close();
    deny_allocation = false;
    if (allocations != diagnostic_before || closes != 4) {
        return 7;
    }
    deny_allocation = true;
    bool polling_failed{};
    try {
        auto records = diagnosed.take_diagnostics();
    } catch (const std::bad_alloc&) {
        polling_failed = true;
    }
    deny_allocation = false;
    if (!polling_failed || diagnosed.take_diagnostics().size() != 1) {
        return 8;
    }
    return diagnosed.take_diagnostics().empty() ? 0 : 9;
}
} // namespace
int main() {
    try {
        return allocation_checks();
    } catch (...) {
        deny_allocation = false;
        return 99;
    }
}
