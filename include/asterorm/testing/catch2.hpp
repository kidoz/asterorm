#pragma once
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_case_info.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include <concepts>
#include <cstdio>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

#include "asterorm/testing/transactional_session.hpp"

namespace asterorm::testing::catch2 {
namespace detail {
inline char lower_ascii(char letter) noexcept {
    return letter >= 'A' && letter <= 'Z' ? static_cast<char>(letter + ('a' - 'A')) : letter;
}
inline bool same_tag(std::string_view left, std::string_view right) noexcept {
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t index = 0; index != left.size(); ++index) {
        if (lower_ascii(left[index]) != lower_ascii(right[index])) {
            return false;
        }
    }
    return true;
}
inline bool valid_name(std::string_view name) noexcept {
    if (name.empty()) {
        return false;
    }
    for (char letter : name) {
        if (!((letter >= 'a' && letter <= 'z') || (letter >= '0' && letter <= '9') ||
              letter == '-' || letter == '_')) {
            return false;
        }
    }
    return true;
}

struct provider {
    std::string name;
    bool poisoned{};
    explicit provider(std::string key) : name(std::move(key)) {}
    virtual ~provider() = default;
    virtual const std::type_info& fixture_type() const noexcept = 0;
    virtual result<void> enter() = 0;
    virtual result<void> finish() = 0;
    virtual void abandon() noexcept = 0;
};

template <typename Fixture> struct fixture_provider final : provider {
    using session_type = std::remove_reference_t<decltype(std::declval<Fixture&>().session())>;
    using scope_type = transactional_session<session_type>;
    std::function<std::unique_ptr<Fixture>()> factory;
    std::unique_ptr<Fixture> fixture;
    std::optional<scope_type> scope;

    fixture_provider(std::string key, std::function<std::unique_ptr<Fixture>()> create)
        : provider(std::move(key)), factory(std::move(create)) {
        static_assert(std::is_lvalue_reference_v<decltype(std::declval<Fixture&>().session())>,
                      "Fixture::session() must return a borrowed session reference");
        static_assert(std::is_nothrow_destructible_v<Fixture>,
                      "A transactional fixture must have nonthrowing destruction");
    }
    const std::type_info& fixture_type() const noexcept override {
        return typeid(Fixture);
    }
    static constexpr bool supported_session() {
        if constexpr (requires { typename session_type::connection_type; }) {
            return asterorm::detail::supports_testing_scope<
                typename session_type::connection_type>();
        }
        return false;
    }
    result<void> enter() override {
        if constexpr (!supported_session()) {
            return std::unexpected(asterorm::detail::testing_scope_error(
                "Fixture requires a session with PostgreSQL/SQLite transactional test "
                "capabilities"));
        }
        if (poisoned) {
            return std::unexpected(asterorm::detail::testing_scope_error(
                "Fixture is poisoned; preparation and connection setup are not retried"));
        }
        if (!fixture) {
            fixture = factory();
            if (!fixture) {
                throw std::logic_error("Fixture factory returned null");
            }
            auto prepared = fixture->prepare(fixture->session());
            static_assert(std::same_as<decltype(prepared), result<void>>,
                          "Fixture::prepare(Session&) must return result<void>");
            if (!prepared) {
                return prepared;
            }
        }
        auto opened = scope_type::create(fixture->session());
        if (!opened) {
            return std::unexpected(std::move(opened.error()));
        }
        scope.emplace(std::move(*opened));
        return {};
    }
    result<void> finish() override {
        if (!scope) {
            return {};
        }
        auto finished = scope->finish();
        scope.reset();
        return finished;
    }
    void abandon() noexcept override {
        scope.reset();
    }
};

struct registry {
    std::vector<std::unique_ptr<provider>> providers;
    provider* active{};
    bool used{};
    std::size_t failures{};
    std::string_view current_case;
    std::string_view first_case;
    std::string_view first_fixture;
    std::string_view first_phase;
    std::string_view first_reason;
    std::optional<db_error> first_error;
    std::exception_ptr first_exception;

    void fail(std::string_view phase, std::string_view reason, provider* selected = nullptr,
              std::optional<db_error> error = {}, std::exception_ptr exception = {}) noexcept {
        if (selected) {
            selected->poisoned = true;
        }
        if (failures++ == 0) {
            first_case = current_case;
            first_fixture = selected ? std::string_view{selected->name} : std::string_view{};
            first_phase = phase;
            first_reason = reason;
            first_error = std::move(error);
            first_exception = std::move(exception);
        }
    }
    void finish() noexcept {
        auto* selected = std::exchange(active, nullptr);
        if (!selected) {
            return;
        }
        try {
            auto finished = selected->finish();
            if (!finished) {
                fail("cleanup", "Scope cleanup failed", selected, std::move(finished.error()));
            }
        } catch (...) {
            fail("cleanup", "Scope cleanup threw", selected, {}, std::current_exception());
        }
        selected->abandon();
    }
    void start(const Catch::TestCaseInfo& info) noexcept {
        finish();
        current_case = info.name;
        provider* selected = nullptr;
        try {
            constexpr std::string_view prefix = "transactional:";
            std::optional<std::string_view> key;
            for (const auto& tag : info.tags) {
                const std::string_view text{tag.original.data(), tag.original.size()};
                if (same_tag(text, "transactional") ||
                    (text.size() >= prefix.size() &&
                     same_tag(text.substr(0, prefix.size()), prefix))) {
                    if (key) {
                        fail("selection", "Multiple transactional fixture tags");
                        return;
                    }
                    key = text.size() >= prefix.size() ? text.substr(prefix.size()) : "";
                }
            }
            if (!key) {
                return;
            }
            for (const auto& candidate : providers) {
                if (same_tag(candidate->name, *key)) {
                    selected = candidate.get();
                    break;
                }
            }
            if (!selected) {
                fail("selection", "Unknown or empty transactional fixture tag");
                return;
            }
            auto entered = selected->enter();
            if (!entered) {
                fail("setup", "Fixture preparation or scope entry failed", selected,
                     std::move(entered.error()));
                selected->abandon();
                return;
            }
            active = selected;
        } catch (...) {
            fail("setup", "Fixture preparation or scope entry threw", selected, {},
                 std::current_exception());
            if (selected) {
                selected->abandon();
            }
        }
    }
    void report() const {
        if (failures == 0) {
            return;
        }
        std::cerr << "AsterORM transactional harness: " << failures << " failure(s); first "
                  << first_phase << ": " << first_reason;
        if (!first_case.empty()) {
            std::cerr << "; case=" << first_case;
        }
        if (!first_fixture.empty()) {
            std::cerr << "; fixture=" << first_fixture;
        }
        if (first_error) {
            std::cerr << "; " << first_error->message;
            if (!first_error->sqlstate.empty()) {
                std::cerr << "; SQLSTATE=" << first_error->sqlstate;
            }
            if (first_error->detail) {
                std::cerr << "; " << *first_error->detail;
            }
        }
        if (first_exception) {
            try {
                std::rethrow_exception(first_exception);
            } catch (const std::exception& error) {
                std::cerr << "; " << error.what();
            } catch (...) {
                std::cerr << "; non-standard C++ exception";
            }
        }
        std::cerr << '\n';
    }
};
inline registry& fixtures() {
    static registry registered;
    return registered;
}
inline thread_local registry* current_context = nullptr;

class listener final : public Catch::EventListenerBase {
  public:
    using Catch::EventListenerBase::EventListenerBase;
    void testCasePartialStarting(const Catch::TestCaseInfo& info,
                                 [[maybe_unused]] std::uint64_t part_number) noexcept override {
        if (current_context) {
            current_context->start(info);
        }
    }
    void testCasePartialEnded([[maybe_unused]] const Catch::TestCaseStats& stats,
                              [[maybe_unused]] std::uint64_t part_number) noexcept override {
        if (current_context) {
            current_context->finish();
        }
    }
};
} // namespace detail

// Register before run(); construction and checked preparation are lazy.
template <typename Fixture>
result<void> register_fixture(
    std::string name, std::function<std::unique_ptr<Fixture>()> factory = [] {
        return std::make_unique<Fixture>();
    }) {
    auto& registered = detail::fixtures();
    if (registered.used || !detail::valid_name(name)) {
        registered.fail("registration", "Registration is closed or fixture name is invalid");
        return std::unexpected(asterorm::detail::testing_scope_error(
            "Register before run with a nonempty lowercase ASCII fixture name"));
    }
    for (const auto& candidate : registered.providers) {
        if (candidate->name == name || candidate->fixture_type() == typeid(Fixture)) {
            registered.fail("registration", "Duplicate fixture name or type");
            return std::unexpected(
                asterorm::detail::testing_scope_error("Duplicate fixture name or type"));
        }
    }
    registered.providers.push_back(
        std::make_unique<detail::fixture_provider<Fixture>>(std::move(name), std::move(factory)));
    return {};
}

template <typename Fixture> auto& session() {
    auto* context = detail::current_context;
    if (context && context->active && !context->active->poisoned) {
        if (auto* selected = dynamic_cast<detail::fixture_provider<Fixture>*>(context->active)) {
            return selected->scope->session();
        }
    }
    if (context) {
        context->fail("access", "No active matching transactional fixture", context->active);
    }
    throw std::logic_error("No active matching transactional fixture on this thread");
}

// One invocation per process. Link Catch2 without its main and call this from yours.
inline int run(int argc, const char* const argv[]) {
    auto& registered = detail::fixtures();
    if (registered.used) {
        std::cerr << "AsterORM transactional harness: run() may only be called once\n";
        return 1;
    }
    registered.used = true;
    int status = 1;
    try {
        const Catch::ListenerRegistrar<detail::listener> registration{
            Catch::StringRef{"asterorm::testing::catch2::listener"}};
        Catch::Session runner;
        detail::current_context = &registered;
        try {
            status = runner.run(argc, argv);
        } catch (...) {
            registered.fail("runner", "Catch2 runner threw", nullptr, {}, std::current_exception());
        }
        registered.finish();
        detail::current_context = nullptr;
        registered.report(); // Catch's test names remain alive until runner destruction.
    } catch (...) {
        registered.finish();
        detail::current_context = nullptr;
        std::fputs("AsterORM transactional harness: runner initialization or reporting failed\n",
                   stderr);
        return status == 0 ? 1 : status;
    }
    return status != 0 ? status : (registered.failures != 0 ? 1 : 0);
}
} // namespace asterorm::testing::catch2
