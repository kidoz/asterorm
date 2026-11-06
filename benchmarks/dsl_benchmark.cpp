#include "measurement_support.hpp"
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "asterorm/sql/compiler.hpp"
#include "asterorm/sql/typed.hpp"

namespace {
struct benchmark_user {
    int id;
    std::string email;
    bool active;
    std::optional<std::string> deleted_at;
};
} // namespace

template <> struct asterorm::entity_traits<benchmark_user> {
    static constexpr const char* table = "benchmark_users";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&benchmark_user::id>("id"),
                        asterorm::column<&benchmark_user::email>("email"),
                        asterorm::column<&benchmark_user::active>("active"),
                        asterorm::column<&benchmark_user::deleted_at>("deleted_at"));
};

namespace {
namespace sql = asterorm::sql;
namespace typed = sql::typed;
using builder_function = sql::select_ast (*)(std::size_t);
enum class phase { build, compile, build_compile };
constexpr std::array phases{phase::build, phase::compile, phase::build_compile};
constexpr std::array dialects{sql::dialect::postgres, sql::dialect::mysql};

sql::select_ast typed_simple(std::size_t input) {
    return typed::select<benchmark_user>()
        .where(typed::col<&benchmark_user::id> > typed::val(18 + static_cast<int>(input % 5)))
        .limit(50)
        .build();
}
sql::select_ast string_simple(std::size_t input) {
    return sql::select({"id", "email", "active", "deleted_at"})
        .from("benchmark_users")
        .where(sql::col("id") > sql::val(18 + static_cast<int>(input % 5)))
        .limit(50)
        .build();
}
std::string excluded_email(std::size_t input) {
    return "blocked-address@benchmark.example/" + std::to_string(input % 8);
}
sql::select_ast typed_nullable(std::size_t input) {
    return typed::select_cols(typed::col<&benchmark_user::email>,
                              typed::col<&benchmark_user::active>,
                              typed::col<&benchmark_user::deleted_at>)
        .where((typed::col<&benchmark_user::active> == typed::val(true)) &&
               typed::is_null(typed::col<&benchmark_user::deleted_at>) &&
               (typed::col<&benchmark_user::email> != typed::val(excluded_email(input))))
        .order_by(typed::col<&benchmark_user::email>)
        .limit(50)
        .offset(input % 4)
        .build();
}
sql::select_ast string_nullable(std::size_t input) {
    return sql::select({"email", "active", "deleted_at"})
        .from("benchmark_users")
        .where((sql::col("active") == sql::val(true)) && sql::is_null(sql::col("deleted_at")) &&
               (sql::col("email") != sql::val(excluded_email(input))))
        .order_by("email")
        .limit(50)
        .offset(input % 4)
        .build();
}
sql::select_ast typed_aggregate(std::size_t input) {
    return typed::select_cols(typed::col<&benchmark_user::active>, typed::count_all(),
                              typed::sum(typed::col<&benchmark_user::id>),
                              typed::avg(typed::col<&benchmark_user::id>))
        .where(typed::col<&benchmark_user::id> > typed::val(18 + static_cast<int>(input % 5)))
        .group_by(typed::col<&benchmark_user::active>)
        .order_by(typed::col<&benchmark_user::active>)
        .build();
}
sql::select_ast string_aggregate(std::size_t input) {
    return sql::select_builder{}
        .from("benchmark_users")
        .items({{"active", nullptr, std::nullopt},
                {std::nullopt, sql::count_all(), std::nullopt},
                {std::nullopt, sql::sum(sql::col("id")), std::nullopt},
                {std::nullopt, sql::avg(sql::col("id")), std::nullopt}})
        .where(sql::col("id") > sql::val(18 + static_cast<int>(input % 5)))
        .group_by({"active"})
        .order_by("active")
        .build();
}
struct workload {
    std::string_view name;
    builder_function typed_builder;
    builder_function string_builder;
};
constexpr std::array workloads{
    workload{"simple_filter", typed_simple, string_simple},
    workload{"nullable_projection", typed_nullable, string_nullable},
    workload{"aggregate", typed_aggregate, string_aggregate},
};

std::string_view phase_name(phase selected) {
    switch (selected) {
    case phase::build:
        return "build";
    case phase::compile:
        return "compile";
    case phase::build_compile:
        return "build_compile";
    }
    throw std::logic_error("invalid benchmark phase");
}

void run_batch(builder_function build, phase selected, sql::dialect target,
               const sql::select_ast& prebuilt, std::size_t iterations) {
    sql::compiler compiler{target};
    switch (selected) {
    case phase::build:
        for (std::size_t index = 0; index < iterations; ++index) {
            auto ast = build(index);
            asterorm::bench::observe(&ast);
        }
        break;
    case phase::compile:
        for (std::size_t index = 0; index < iterations; ++index) {
            auto query = compiler.compile(prebuilt).value();
            asterorm::bench::observe(&query);
        }
        break;
    case phase::build_compile:
        for (std::size_t index = 0; index < iterations; ++index) {
            auto ast = build(index);
            auto query = compiler.compile(ast).value();
            asterorm::bench::observe(&query);
        }
        break;
    }
}

void verify_queries() {
    for (const auto& current : workloads) {
        for (auto target : dialects) {
            sql::compiler compiler{target};
            for (std::size_t input = 0; input < 32; ++input) {
                auto typed_query = compiler.compile(current.typed_builder(input));
                auto string_query = compiler.compile(current.string_builder(input));
                if (!typed_query || !string_query || typed_query->sql != string_query->sql ||
                    typed_query->params != string_query->params) {
                    throw std::runtime_error("SQL/parameter mismatch: " +
                                             std::string(current.name));
                }
            }
        }
    }
}

std::size_t parse_count(std::string_view text) {
    std::size_t count{};
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), count);
    if (error != std::errc{} || end != text.data() + text.size() || count == 0 ||
        count > 10000000) {
        throw std::invalid_argument("counts must be integers in [1, 10000000]");
    }
    return count;
}

int run(int argument_count, char** arguments) {
    std::size_t iterations = 10000;
    std::size_t samples = 21;
    std::size_t seed = 1;
    bool verify_only = false;
    bool describe_asts = false;
    for (int index = 1; index < argument_count; ++index) {
        const std::string_view option{arguments[index]};
        if (option == "--verify-only") {
            verify_only = true;
        } else if (option == "--describe-asts") {
            describe_asts = true;
        } else if ((option == "--iterations" || option == "--samples" || option == "--seed") &&
                   index + 1 < argument_count) {
            const auto count = parse_count(arguments[++index]);
            if (option == "--iterations")
                iterations = count;
            else if (option == "--samples")
                samples = count;
            else
                seed = count;
        } else {
            throw std::invalid_argument(
                "usage: --verify-only | --describe-asts | --iterations N --samples N --seed N");
        }
    }
    verify_queries();
    if (describe_asts) {
        std::cout << "workload,path,columns_size,columns_capacity,items_size,items_capacity,"
                     "group_by_capacity,string_size,select_item_size\n";
        for (const auto& current : workloads) {
            for (bool use_typed : {false, true}) {
                auto ast = (use_typed ? current.typed_builder : current.string_builder)(0);
                std::cout << current.name << ',' << (use_typed ? "typed" : "string") << ','
                          << ast.columns.size() << ',' << ast.columns.capacity() << ','
                          << ast.items.size() << ',' << ast.items.capacity() << ','
                          << ast.group_bys.capacity() << ',' << sizeof(std::string) << ','
                          << sizeof(sql::select_item) << '\n';
            }
        }
        return 0;
    }
#ifdef ASTERORM_BENCH_ALLOCATIONS
    asterorm::bench::verify_allocation_probe();
#endif
    if (verify_only) {
        std::cout << "Verified 192 SQL/parameter pairs";
#ifdef ASTERORM_BENCH_ALLOCATIONS
        std::cout << " and allocation probe calibration";
#endif
        std::cout << '\n';
        return 0;
    }
    std::cout << "workload,dialect,phase,sample,path,iterations,ns_per_query,allocations_per_query,"
                 "bytes_per_query\n"
              << std::fixed << std::setprecision(3);
    for (const auto& current : workloads) {
        for (auto target : dialects) {
            for (auto selected : phases) {
                // Prebuilt ASTs, warmup, and output are outside measurement.
                auto typed_ast = current.typed_builder(0);
                auto string_ast = current.string_builder(0);
                run_batch(current.typed_builder, selected, target, typed_ast, 1000);
                run_batch(current.string_builder, selected, target, string_ast, 1000);
                for (std::size_t sample = 0; sample < samples; ++sample) {
                    for (std::size_t position = 0; position < 2; ++position) {
                        const bool use_typed = (position + sample + seed) % 2 == 0;
                        auto build = use_typed ? current.typed_builder : current.string_builder;
                        const auto& prebuilt = use_typed ? typed_ast : string_ast;
#ifdef ASTERORM_BENCH_ALLOCATIONS
                        asterorm::bench::begin_allocations();
                        run_batch(build, selected, target, prebuilt, iterations);
                        const auto counts = asterorm::bench::end_allocations();
#else
                        const auto started = std::chrono::steady_clock::now();
                        run_batch(build, selected, target, prebuilt, iterations);
                        const auto elapsed = std::chrono::steady_clock::now() - started;
                        const auto nanoseconds =
                            std::chrono::duration<double, std::nano>(elapsed).count();
#endif
                        std::cout << current.name << ','
                                  << (target == sql::dialect::postgres ? "postgres_sqlite"
                                                                       : "mysql")
                                  << ',' << phase_name(selected) << ',' << sample << ','
                                  << (use_typed ? "typed" : "string") << ',' << iterations << ',';
#ifdef ASTERORM_BENCH_ALLOCATIONS
                        std::cout << ','
                                  << static_cast<double>(counts.calls) /
                                         static_cast<double>(iterations)
                                  << ','
                                  << static_cast<double>(counts.bytes) /
                                         static_cast<double>(iterations);
#else
                        std::cout << nanoseconds / static_cast<double>(iterations) << ",,";
#endif
                        std::cout << '\n';
                    }
                }
            }
        }
    }
    return 0;
}
} // namespace

int main(int argument_count, char** arguments) {
    try {
        return run(argument_count, arguments);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
