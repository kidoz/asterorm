#include "measurement_support.hpp"
#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "asterorm/core/repository.hpp"
#include "asterorm/mysql/result.hpp"
#include "asterorm/postgres/result.hpp"
#include "asterorm/sql/typed_query.hpp"
#include "asterorm/sqlite/result.hpp"

namespace {
struct decode_entity {
    std::int64_t id;
    std::string label;
    bool active;
    double score;
    std::optional<std::int64_t> optional_id;
    std::optional<std::string> note;
    std::optional<double> weight;
    asterorm::numeric amount;
};
} // namespace

template <> struct asterorm::entity_traits<decode_entity> {
    static constexpr const char* table = "decode_fixture";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&decode_entity::id>("id"),
                        asterorm::column<&decode_entity::label>("label"),
                        asterorm::column<&decode_entity::active>("active"),
                        asterorm::column<&decode_entity::score>("score"),
                        asterorm::column<&decode_entity::optional_id>("optional_id"),
                        asterorm::column<&decode_entity::note>("note"),
                        asterorm::column<&decode_entity::weight>("weight"),
                        asterorm::column<&decode_entity::amount>("amount"));
};

namespace {
namespace typed = asterorm::sql::typed;
using raw_row = std::vector<std::optional<std::string>>;
using raw_rows = std::vector<raw_row>;
using native_result =
    std::variant<asterorm::pg::result, asterorm::sqlite::result, asterorm::mysql::result>;
constexpr std::array<std::string_view, 3> backends{"postgres", "sqlite", "mysql"};
constexpr std::array<std::size_t, 3> row_counts{1, 256, 16384};
constexpr std::string_view decimal_text = "123456789012345678901234567890.1250";

std::string payload(std::size_t index) {
    return "row-decoding-payload-abcdefghijklmnopqrstuvwxyz-" + std::to_string(index % 97);
}
std::string boolean_text(std::string_view backend, std::size_t index) {
    return backend == "postgres" ? (index % 2 == 0 ? "t" : "f") : (index % 2 == 0 ? "1" : "0");
}

struct scalar_shape {
    static constexpr std::string_view name = "scalars";
    using selection = decltype(typed::select_cols(
        typed::col<&decode_entity::id>, typed::col<&decode_entity::label>,
        typed::col<&decode_entity::active>, typed::col<&decode_entity::score>));
    using row_type = typename selection::result_type;
    static std::vector<std::string> names() {
        return {"id", "label", "active", "score"};
    }
    static std::array<Oid, 4> oids() {
        return {20, 25, 16, 701};
    }
    static raw_row encoded(std::string_view backend, std::size_t index) {
        return {std::to_string(index + 1), payload(index), boolean_text(backend, index),
                index % 2 == 0 ? "12.5" : "-4.25"};
    }
    static row_type expected(std::size_t index) {
        return {static_cast<std::int64_t>(index + 1), payload(index), index % 2 == 0,
                index % 2 == 0 ? 12.5 : -4.25};
    }
};

struct nullable_shape {
    static constexpr std::string_view name = "nullable";
    using selection = decltype(typed::select_cols(
        typed::col<&decode_entity::optional_id>, typed::col<&decode_entity::note>,
        typed::col<&decode_entity::active>, typed::col<&decode_entity::weight>));
    using row_type = typename selection::result_type;
    static std::vector<std::string> names() {
        return {"optional_id", "note", "active", "weight"};
    }
    static std::array<Oid, 4> oids() {
        return {20, 25, 16, 701};
    }
    static raw_row encoded(std::string_view backend, std::size_t index) {
        return {index % 3 == 0 ? std::nullopt
                               : std::optional<std::string>{std::to_string(index + 1)},
                index % 2 == 0 ? std::nullopt
                               : std::optional<std::string>{index % 4 == 1 ? "" : payload(index)},
                boolean_text(backend, index),
                index % 5 == 0 ? std::nullopt : std::optional<std::string>{"1.25"}};
    }
    static row_type expected(std::size_t index) {
        return {index % 3 == 0 ? std::nullopt
                               : std::optional<std::int64_t>{static_cast<std::int64_t>(index + 1)},
                index % 2 == 0 ? std::nullopt
                               : std::optional<std::string>{index % 4 == 1 ? "" : payload(index)},
                index % 2 == 0, index % 5 == 0 ? std::nullopt : std::optional<double>{1.25}};
    }
};

struct aggregate_shape {
    static constexpr std::string_view name = "aggregates";
    using selection =
        decltype(typed::select_cols(typed::count_all(), typed::sum(typed::col<&decode_entity::id>),
                                    typed::avg(typed::col<&decode_entity::score>),
                                    typed::sum(typed::col<&decode_entity::amount>)));
    using row_type = typename selection::result_type;
    static std::vector<std::string> names() {
        return {"count", "sum", "avg", "numeric_sum"};
    }
    static std::array<Oid, 4> oids() {
        return {20, 1700, 701, 1700};
    }
    static raw_row encoded(std::string_view backend, std::size_t index) {
        return {std::to_string(100 + index % 3), std::to_string(300 + index % 7),
                backend == "postgres" ? "12.5000000000000000"
                                      : (backend == "mysql" ? "12.5000" : "12.5"),
                std::string{decimal_text}};
    }
    static row_type expected(std::size_t index) {
        return {static_cast<std::int64_t>(100 + index % 3),
                static_cast<std::int64_t>(300 + index % 7), 12.5,
                asterorm::numeric{std::string{decimal_text}}};
    }
};

// These are native result wrappers populated before timing, not database queries.
native_result make_fixture(std::string_view backend, std::vector<std::string> names,
                           const std::array<Oid, 4>& oids, raw_rows rows) {
    if (backend == "sqlite") {
        return asterorm::sqlite::result{std::move(names), std::move(rows), 0};
    }
    if (backend == "mysql") {
        return asterorm::mysql::result{std::move(names), std::move(rows), 0};
    }
    std::unique_ptr<PGresult, decltype(&PQclear)> result{
        PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), PQclear};
    if (!result) {
        throw std::runtime_error("PQmakeEmptyPGresult failed");
    }
    std::array<PGresAttDesc, 4> attributes{};
    for (std::size_t column = 0; column < attributes.size(); ++column) {
        attributes[column].name = names[column].data();
        attributes[column].typid = oids[column];
        attributes[column].typlen = -1;
        attributes[column].atttypmod = -1;
        attributes[column].format = 0;
    }
    if (PQsetResultAttrs(result.get(), 4, attributes.data()) == 0) {
        throw std::runtime_error("PQsetResultAttrs failed");
    }
    for (std::size_t row = 0; row < rows.size(); ++row) {
        for (std::size_t column = 0; column < 4; ++column) {
            auto& value = rows[row][column];
            if (PQsetvalue(result.get(), static_cast<int>(row), static_cast<int>(column),
                           value ? value->data() : nullptr,
                           value ? static_cast<int>(value->size()) : -1) == 0) {
                throw std::runtime_error("PQsetvalue failed");
            }
        }
    }
    return asterorm::pg::result{result.release()};
}

template <typename Shape> native_result fixture(std::string_view backend, std::size_t count) {
    raw_rows rows;
    rows.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        rows.push_back(Shape::encoded(backend, index));
    }
    return make_fixture(backend, Shape::names(), Shape::oids(), std::move(rows));
}

template <typename Entity, typename Projection, typename... Items, typename Result>
asterorm::result<Projection>
typed_row(const Result& result, int row,
          std::type_identity<typed::typed_select_builder<Entity, Projection, Items...>>) {
    return typed::detail::hydrate_dto<Projection, Items...>(result, row);
}

template <bool Typed, typename Shape, typename Result>
asterorm::result<typename Shape::row_type> decode_row(const Result& result, int row) {
    if constexpr (Typed) {
        return typed_row(result, row, std::type_identity<typename Shape::selection>{});
    } else {
        return asterorm::detail::decode_projection<typename Shape::row_type>(result, row);
    }
}

template <bool Typed, typename Shape, typename Result>
asterorm::result<std::vector<typename Shape::row_type>> decode_vector(const Result& result) {
    std::vector<typename Shape::row_type> rows;
    rows.reserve(static_cast<std::size_t>(result.rows()));
    for (int row = 0; row < result.rows(); ++row) {
        auto decoded = decode_row<Typed, Shape>(result, row);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        rows.push_back(std::move(*decoded));
    }
    return rows;
}

template <typename Shape, typename Result> void verify_values(const Result& result) {
    auto typed_rows = decode_vector<true, Shape>(result);
    auto string_rows = decode_vector<false, Shape>(result);
    if (!typed_rows || !string_rows || *typed_rows != *string_rows ||
        typed_rows->size() != static_cast<std::size_t>(result.rows())) {
        throw std::runtime_error("decoded values differ between APIs");
    }
    for (std::size_t row = 0; row < typed_rows->size(); ++row) {
        if ((*typed_rows)[row] != Shape::expected(row)) {
            throw std::runtime_error("decoded values differ from the fixture oracle");
        }
    }
}

template <typename Shape> void verify_failures(std::string_view backend) {
    for (auto invalid : {std::optional<std::string>{}, std::optional<std::string>{"invalid"}}) {
        auto row = Shape::encoded(backend, 0);
        // Nullable first columns legitimately accept NULL; its required bool must reject it.
        if constexpr (std::is_same_v<Shape, nullable_shape>) {
            row[2] = invalid;
        } else {
            row[0] = invalid;
        }
        auto result = make_fixture(backend, Shape::names(), Shape::oids(), {std::move(row)});
        std::visit(
            [](const auto& native) {
                auto typed_value = decode_row<true, Shape>(native, 0);
                auto string_value = decode_row<false, Shape>(native, 0);
                if (typed_value || string_value ||
                    typed_value.error().kind != asterorm::db_error_kind::parse_failed ||
                    typed_value.error().kind != string_value.error().kind ||
                    typed_value.error().message != string_value.error().message) {
                    throw std::runtime_error("error propagation differs between APIs");
                }
            },
            result);
    }
}

template <bool Typed, typename Shape, typename Result>
void run_batch(const Result& result, bool materialize, std::size_t repetitions) {
    for (std::size_t repeat = 0; repeat < repetitions; ++repeat) {
        if (materialize) {
            auto rows = decode_vector<Typed, Shape>(result).value();
            asterorm::bench::observe(&rows);
        } else {
            for (int row = 0; row < result.rows(); ++row) {
                auto decoded = decode_row<Typed, Shape>(result, row).value();
                asterorm::bench::observe(&decoded);
            }
        }
    }
}

struct options {
    std::size_t target_rows{8192};
    std::size_t samples{21};
    std::size_t seed{1};
    bool verify_only{false};
};

template <typename Shape>
void measure_shape(std::string_view backend, const options& configuration) {
    auto empty = fixture<Shape>(backend, 0);
    std::visit([](const auto& result) { verify_values<Shape>(result); }, empty);
    verify_failures<Shape>(backend);
    for (auto row_count : row_counts) {
        auto result = fixture<Shape>(backend, row_count);
        std::visit(
            [&](const auto& native) {
                verify_values<Shape>(native);
                if (configuration.verify_only) {
                    return;
                }
                for (bool materialize : {false, true}) {
                    const auto repetitions =
                        std::max<std::size_t>(1, configuration.target_rows / row_count);
                    const auto measured_rows = repetitions * row_count;
                    const auto warmup = std::max<std::size_t>(1, 1024 / row_count);
                    run_batch<true, Shape>(native, materialize, warmup);
                    run_batch<false, Shape>(native, materialize, warmup);
                    for (std::size_t sample = 0; sample < configuration.samples; ++sample) {
                        for (std::size_t position = 0; position < 2; ++position) {
                            const bool use_typed =
                                (position + sample + configuration.seed) % 2 == 0;
                            auto invoke = [&]() {
                                if (use_typed) {
                                    run_batch<true, Shape>(native, materialize, repetitions);
                                } else {
                                    run_batch<false, Shape>(native, materialize, repetitions);
                                }
                            };
#ifdef ASTERORM_BENCH_ALLOCATIONS
                            asterorm::bench::begin_allocations();
                            invoke();
                            const auto counts = asterorm::bench::end_allocations();
#else
                            const auto started = std::chrono::steady_clock::now();
                            invoke();
                            const auto elapsed = std::chrono::steady_clock::now() - started;
                            const auto nanoseconds =
                                std::chrono::duration<double, std::nano>(elapsed).count();
#endif
                            std::cout << backend << ',' << Shape::name << ',' << row_count << ','
                                      << (materialize ? "result_vector" : "row_decode") << ','
                                      << sample << ',' << (use_typed ? "typed" : "string") << ','
                                      << measured_rows << ',';
#ifdef ASTERORM_BENCH_ALLOCATIONS
                            std::cout << ','
                                      << static_cast<double>(counts.calls) /
                                             static_cast<double>(measured_rows)
                                      << ','
                                      << static_cast<double>(counts.bytes) /
                                             static_cast<double>(measured_rows);
#else
                            std::cout << nanoseconds / static_cast<double>(measured_rows) << ",,";
#endif
                            std::cout << '\n';
                        }
                    }
                }
            },
            result);
    }
}

std::size_t parse_count(std::string_view text) {
    std::size_t count{};
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), count);
    if (error != std::errc{} || end != text.data() + text.size() || count == 0 ||
        count > 10000000) {
        throw std::invalid_argument("counts must be in [1, 10000000]");
    }
    return count;
}

int run(int argument_count, char** arguments) {
    options configuration;
    for (int index = 1; index < argument_count; ++index) {
        const std::string_view option{arguments[index]};
        if (option == "--verify-only") {
            configuration.verify_only = true;
        } else if (index + 1 < argument_count &&
                   (option == "--target-rows" || option == "--samples" || option == "--seed")) {
            auto count = parse_count(arguments[++index]);
            if (option == "--target-rows") {
                configuration.target_rows = count;
            } else if (option == "--samples") {
                configuration.samples = count;
            } else {
                configuration.seed = count;
            }
        } else {
            throw std::invalid_argument("invalid decoding benchmark arguments");
        }
    }
#ifdef ASTERORM_BENCH_ALLOCATIONS
    asterorm::bench::verify_allocation_probe();
#endif
    if (!configuration.verify_only) {
        std::cout << "backend,workload,row_count,phase,sample,path,decoded_rows,ns_per_row,"
                     "allocations_per_row,bytes_per_row\n"
                  << std::fixed << std::setprecision(6);
    }
    for (auto backend : backends) {
        measure_shape<scalar_shape>(backend, configuration);
        measure_shape<nullable_shape>(backend, configuration);
        measure_shape<aggregate_shape>(backend, configuration);
    }
    if (configuration.verify_only) {
        std::cout << "Verified native PostgreSQL/SQLite/MySQL fixtures: all values, empty results, "
                     "NULL/invalid required-field errors; sizes 1, 256, 16384; libpq="
                  << PQlibVersion() << '\n';
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
