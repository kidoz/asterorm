#pragma once
#include <asterorm/session.hpp>
#include <asterorm/sql/query.hpp>
#include <asterorm/sql/typed_extensions.hpp>

namespace asterorm::acceptance {
struct walk_node {
    int id{};
    std::optional<int> parent;
    std::optional<int> value;
};
struct composed_row {
    int id{};
    std::optional<int> parent;
    std::int64_t number{};
    std::optional<std::int64_t> total;
    std::optional<int> previous;
    bool operator==(const composed_row&) const = default;
};
} // namespace asterorm::acceptance
template <> struct asterorm::entity_traits<asterorm::acceptance::walk_node> {
    static constexpr const char* table = "walk";
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&asterorm::acceptance::walk_node::id>("id"),
                        asterorm::column<&asterorm::acceptance::walk_node::parent>("parent"),
                        asterorm::column<&asterorm::acceptance::walk_node::value>("value"));
};
template <> struct asterorm::entity_traits<asterorm::acceptance::composed_row> {
    // Reversed mapping order exercises named materialization after the compound.
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&asterorm::acceptance::composed_row::previous>("previous"),
                        asterorm::column<&asterorm::acceptance::composed_row::total>("total"),
                        asterorm::column<&asterorm::acceptance::composed_row::number>("number"),
                        asterorm::column<&asterorm::acceptance::composed_row::parent>("parent"),
                        asterorm::column<&asterorm::acceptance::composed_row::id>("id"));
};
namespace asterorm::acceptance {
inline sql::select_ast ranked_walk(bool filtered, bool typed) {
    namespace typed_sql = sql::typed;
    if (typed) {
        auto order = typed_sql::window().order_by(typed_sql::col<&walk_node::id>);
        auto running = order.rows_between(sql::unbounded_preceding(), sql::current_row());
        auto total = typed_sql::over(typed_sql::sum(typed_sql::col<&walk_node::value>), running);
        if (filtered) {
            total = typed_sql::over(
                typed_sql::filter(typed_sql::sum(typed_sql::col<&walk_node::value>),
                                  typed_sql::col<&walk_node::value> > typed_sql::val(10)),
                running);
        }
        return typed_sql::select_cols(
                   typed_sql::col<&walk_node::id>, typed_sql::col<&walk_node::parent>,
                   typed_sql::over(typed_sql::row_number(), order).as("number"), total.as("total"),
                   typed_sql::over(
                       typed_sql::lag(typed_sql::col<&walk_node::value>, 1, typed_sql::val(-1)),
                       order)
                       .as("previous"))
            .build();
    }
    sql::over_clause order{.partition_bys = {}, .order_bys = {{.column = "id"}}, .rows = {}};
    auto running = order;
    running.rows = sql::rows_between(sql::unbounded_preceding(), sql::current_row());
    auto total = sql::sum(sql::col("value"));
    if (filtered) {
        total = sql::filter(total, sql::col("value") > sql::val(10));
    }
    sql::select_ast selected;
    selected.table = "walk";
    selected.items = {
        {.column_name = "id", .fn = {}, .alias = {}},
        {.column_name = "parent", .fn = {}, .alias = {}},
        {.column_name = {}, .fn = sql::over(sql::row_number(), order), .alias = "number"},
        {.column_name = {}, .fn = sql::over(total, running), .alias = "total"},
        {.column_name = {},
         .fn = sql::over(sql::lag(sql::col("value"), 1, sql::expr_ast{sql::val(-1)}), order),
         .alias = "previous"},
    };
    return selected;
}
inline sql::query_ast composed_query(bool filtered = false, bool distinct = false,
                                     bool typed = false, bool paged = true) {
    auto source = [](const char* table, int first) {
        return sql::query(sql::select({"id", "parent", "value"})
                              .from(table)
                              .where(sql::col("id") == sql::val(first))
                              .build())
            .build();
    };
    auto seeds = sql::union_all(source("acceptance_roots", 1), source("acceptance_more", 9));
    auto anchor = sql::query(sql::select({"id", "parent", "value"})
                                 .from("seeds")
                                 .where(sql::col("id") > sql::val(0))
                                 .build());
    sql::select_ast step;
    step.table = "acceptance_edges";
    step.table_alias = "child";
    step.columns = {"child.id", "child.parent", "child.value"};
    step.joins.push_back({.kind = sql::join_kind::inner,
                          .table = "walk",
                          .alias = "path",
                          .on_clause = sql::col("child.parent") == sql::col("path.id")});
    step.where_clause = sql::col("child.id") > sql::val(0);
    auto ranked = ranked_walk(filtered, typed);
    if (distinct) {
        ranked.distinct_on = sql::distinct_on_clause{
            .columns = {"parent"}, .ties = sql::distinct_on_ties::require_tiebreaker};
        ranked.order_bys = {{.column = "parent"}, {.column = "id", .ascending = false}};
    }
    const std::vector<std::string> labels{"id", "parent", "number", "total", "previous"};
    auto left = sql::query(
        sql::select(labels).from("ranked").where(sql::col("number") <= sql::val(5)).build());
    if (paged) {
        left.order_by("id", false).limit(2).offset(1);
    }
    auto right = sql::query(
        sql::select(labels).from("acceptance_tail").where(sql::col("id") > sql::val(98)).build());
    auto root = sql::union_all(left.build(), right.build());
    root.with_cte("seeds", seeds.build(), {"id", "parent", "value"})
        .with_cte("walk",
                  sql::union_all(anchor.build(), sql::query(std::move(step)).build()).build(),
                  {"id", "parent", "value"})
        .with_cte("ranked", sql::query(std::move(ranked)).build(), labels)
        .recursive()
        .order_by("id");
    if (paged) {
        root.limit(3).offset(1);
    }
    return root.build();
}
template <typename Session> result<void> prepare_composed_rows(Session& db) {
    // Each physical TEMP source appears once, including on MySQL.
    for (const auto* statement : {
             "CREATE TEMPORARY TABLE acceptance_roots(id INTEGER, parent INTEGER, value INTEGER)",
             "CREATE TEMPORARY TABLE acceptance_more(id INTEGER, parent INTEGER, value INTEGER)",
             "CREATE TEMPORARY TABLE acceptance_edges(id INTEGER, parent INTEGER, value INTEGER)",
             "CREATE TEMPORARY TABLE acceptance_tail(id INTEGER, parent INTEGER, number INTEGER, "
             "total INTEGER, previous INTEGER)",
             "INSERT INTO acceptance_roots VALUES(1,NULL,10)",
             "INSERT INTO acceptance_more VALUES(9,NULL,90)",
             "INSERT INTO acceptance_edges VALUES(2,1,20),(3,2,NULL),(4,1,40),(10,9,100)",
             "INSERT INTO acceptance_tail VALUES(99,NULL,9,NULL,NULL),(99,NULL,9,NULL,NULL)",
         }) {
        auto executed =
            db.with_connection([&](auto& connection) { return connection.execute(statement); });
        if (!executed) {
            return std::unexpected(executed.error());
        }
    }
    return {};
}
} // namespace asterorm::acceptance
