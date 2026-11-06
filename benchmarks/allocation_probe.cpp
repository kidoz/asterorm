// Linked only into dsl_allocations. The timing executable uses the normal
// allocator. The core library is static so its allocations use this replacement.
#include "measurement_support.hpp"
#include <cstdlib>
#include <new>
#include <stdexcept>

namespace {
thread_local bool counting = false;
thread_local asterorm::bench::allocation_counts counts;

void* allocate(std::size_t size, std::size_t alignment = 0) {
    const auto requested_size = size;
    size = size == 0 ? 1 : size;
    if (alignment != 0) {
        if (size > static_cast<std::size_t>(-1) - (alignment - 1)) {
            throw std::bad_alloc{};
        }
        size = (size + alignment - 1) / alignment * alignment;
    }
    for (;;) {
        void* memory = alignment == 0 ? std::malloc(size) : std::aligned_alloc(alignment, size);
        if (memory != nullptr) {
            if (counting) {
                ++counts.calls;
                counts.bytes += requested_size;
            }
            return memory;
        }
        auto handler = std::get_new_handler();
        if (handler == nullptr) {
            throw std::bad_alloc{};
        }
        handler();
    }
}
} // namespace

void* operator new(std::size_t size) {
    return allocate(size);
}
void* operator new[](std::size_t size) {
    return allocate(size);
}
void* operator new(std::size_t size, std::align_val_t alignment) {
    return allocate(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    return allocate(size, static_cast<std::size_t>(alignment));
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return ::operator new[](size);
    } catch (...) {
        return nullptr;
    }
}
void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size, alignment);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    try {
        return ::operator new[](size, alignment);
    } catch (...) {
        return nullptr;
    }
}
void operator delete(void* memory) noexcept {
    std::free(memory);
}
void operator delete[](void* memory) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, const std::nothrow_t&) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, const std::nothrow_t&) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::align_val_t, const std::nothrow_t&) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::align_val_t, const std::nothrow_t&) noexcept {
    std::free(memory);
}

void asterorm::bench::begin_allocations() noexcept {
    counts = {};
    counting = true;
}
asterorm::bench::allocation_counts asterorm::bench::end_allocations() noexcept {
    counting = false;
    return counts;
}

void asterorm::bench::verify_allocation_probe() {
    begin_allocations();
    auto* ordinary = ::operator new(64);
    observe(ordinary);
    ::operator delete(ordinary);
    auto* array = ::operator new[](64, std::nothrow);
    const bool array_allocated = array != nullptr;
    observe(array);
    ::operator delete[](array, std::nothrow);
    auto* aligned = ::operator new(80, std::align_val_t{64});
    observe(aligned);
    const bool correctly_aligned = reinterpret_cast<std::uintptr_t>(aligned) % 64 == 0;
    ::operator delete(aligned, std::align_val_t{64});
    auto* aligned_array = ::operator new[](80, std::align_val_t{64}, std::nothrow);
    const bool aligned_array_allocated = aligned_array != nullptr;
    observe(aligned_array);
    ::operator delete[](aligned_array, std::align_val_t{64}, std::nothrow);
    const auto measured = end_allocations();
    if (measured.calls != 4 || measured.bytes != 288 || !correctly_aligned || !array_allocated ||
        !aligned_array_allocated) {
        throw std::runtime_error("allocation probe calibration failed");
    }
}
