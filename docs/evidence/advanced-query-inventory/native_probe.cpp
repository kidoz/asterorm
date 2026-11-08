#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#ifdef ASTERORM_INVENTORY_POSTGRES
#include "asterorm/postgres/driver.hpp"
using inventory_driver = asterorm::pg::driver;
#elifdef ASTERORM_INVENTORY_SQLITE
#include "asterorm/sqlite/driver.hpp"
using inventory_driver = asterorm::sqlite::driver;
#elifdef ASTERORM_INVENTORY_MYSQL
#include "asterorm/mysql/driver.hpp"
using inventory_driver = asterorm::mysql::driver;
#endif
#include "asterorm/sql/compiler.hpp"

namespace {
template <typename Value> Value take(asterorm::result<Value> result) {
    if (!result) {
        throw std::runtime_error("Native inventory command failed; connection settings omitted");
    }
    return std::move(*result);
}
template <typename Result> std::string rows(const Result& result) {
    std::string serialized;
    for (int row = 0; row < result.rows(); ++row) {
        if (row != 0) {
            serialized += '|';
        }
        for (int column = 0; column < result.columns(); ++column) {
            if (column != 0) {
                serialized += ',';
            }
            serialized += result.get_string(row, column).value_or("NULL");
        }
    }
    return serialized;
}
} // namespace

int main() try {
    using connection_type = decltype(inventory_driver{}.connect(std::string{}))::value_type;
    constexpr auto dialect = asterorm::sql::dialect_for<connection_type>();
    constexpr bool postgres = dialect == asterorm::sql::dialect::postgres;
    constexpr bool sqlite = dialect == asterorm::sql::dialect::sqlite;
    const std::string settings = [] {
        if constexpr (sqlite) {
            return std::string{":memory:"};
        } else {
            const auto* configured =
                std::getenv(postgres ? "ASTERORM_TEST_CONNINFO" : "ASTERORM_MYSQL_TEST_CONNINFO");
            if (configured) {
                return std::string{configured};
            }
            if constexpr (postgres) {
                return std::string{"host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                   "password=orm_test sslmode=disable"};
            } else {
                return std::string{
                    "host=127.0.0.1 port=3307 dbname=orm_test user=orm_test password=orm_test"};
            }
        }
    }();
    auto connection = take(inventory_driver{}.connect(settings));
    auto check = [&](const char* name, std::string_view sql, std::string_view expected) {
        auto result = take(connection.execute(sql));
        const auto actual = rows(result);
        if (actual != expected) {
            throw std::runtime_error(std::string{name} + " unexpected result: " + actual);
        }
        std::cout << name << '=' << actual << '\n';
    };
    auto capability = [&](const char* name, std::string_view sql, bool supported,
                          std::string_view expected) {
        auto result = connection.execute(sql);
        if (static_cast<bool>(result) != supported) {
            throw std::runtime_error(std::string{name} + " unexpected capability result");
        }
        if (result) {
            const auto actual = rows(*result);
            if (actual != expected) {
                throw std::runtime_error(std::string{name} + " unexpected rows: " + actual);
            }
            std::cout << name << '=' << actual << '\n';
        } else {
            std::cout << name << "=rejected:" << result.error().sqlstate << ':'
                      << result.error().detail.value_or("") << '\n';
        }
    };
    constexpr auto version_sql = [] {
        if constexpr (postgres) {
            return "SHOW server_version";
        } else if constexpr (sqlite) {
            return "SELECT sqlite_version()";
        } else {
            return "SELECT VERSION()";
        }
    }();
    const auto version = take(connection.execute(version_sql));
    std::cout << "version=" << rows(version) << '\n';
    (void)take(connection.execute(
        "CREATE TEMPORARY TABLE inventory_values(id INTEGER,grp INTEGER,v INTEGER)"));
    (void)take(connection.execute(
        "INSERT INTO inventory_values VALUES(1,1,10),(2,1,10),(3,1,30),(4,2,NULL)"));
    const std::string first = dialect == asterorm::sql::dialect::mysql ? "?" : "$1";
    const std::string second = dialect == asterorm::sql::dialect::mysql ? "?" : "$2";
    const auto bound = take(connection.execute_params(
        "WITH filtered AS (SELECT id FROM inventory_values WHERE id>" + first +
            ") SELECT id FROM filtered WHERE id<" + second + " ORDER BY id",
        {asterorm::encode_parameter(1), asterorm::encode_parameter(4)}));
    if (rows(bound) != "2|3") {
        throw std::runtime_error("CTE/outer binding failed");
    }
    std::cout << "cte_bound=" << rows(bound) << '\n';
    capability("cte_reuse_simple_temp",
               "WITH filtered AS (SELECT id FROM inventory_values WHERE id<=2) "
               "SELECT a.id,b.id FROM filtered a JOIN filtered b ON a.id=b.id ORDER BY a.id",
               dialect != asterorm::sql::dialect::mysql, "1,1|2,2");
    capability("cte_reuse_distinct_temp",
               "WITH filtered AS (SELECT DISTINCT id FROM inventory_values WHERE id<=2) "
               "SELECT a.id,b.id FROM filtered a JOIN filtered b ON a.id=b.id ORDER BY a.id",
               dialect != asterorm::sql::dialect::mysql, "1,1|2,2");
    check("cte_reuse_literals",
          "WITH filtered AS (SELECT 1 AS id UNION ALL SELECT 2 AS id) "
          "SELECT a.id,b.id FROM filtered a JOIN filtered b ON a.id=b.id ORDER BY a.id",
          "1,1|2,2");
    (void)take(
        connection.execute("CREATE TEMPORARY TABLE inventory_edges(id INTEGER,parent_id INTEGER)"));
    (void)take(connection.execute("INSERT INTO inventory_edges VALUES(1,NULL),(2,1),(3,2),(4,1)"));
    (void)take(connection.execute("CREATE TEMPORARY TABLE inventory_roots(id INTEGER)"));
    (void)take(connection.execute("INSERT INTO inventory_roots VALUES(1)"));
    (void)take(connection.execute(
        "CREATE TEMPORARY TABLE inventory_other(id INTEGER,grp INTEGER,v INTEGER)"));
    (void)take(
        connection.execute("INSERT INTO inventory_other VALUES(1,1,10),(3,1,30),(4,2,NULL)"));
    check("recursive_tree",
          "WITH RECURSIVE tree(id) AS (SELECT id FROM inventory_roots "
          "UNION ALL SELECT child.id FROM inventory_edges child JOIN tree ON "
          "child.parent_id=tree.id) "
          "SELECT id FROM tree ORDER BY id",
          "1|2|3|4");
    check("null_order", "SELECT id FROM inventory_values ORDER BY v,id",
          postgres ? "1|2|3|4" : "4|1|2|3");
    check("ranking",
          "SELECT id,row_number() OVER(PARTITION BY grp ORDER BY v,id), "
          "rank() OVER(PARTITION BY grp ORDER BY v),dense_rank() OVER(PARTITION BY grp ORDER BY v) "
          "FROM inventory_values ORDER BY id",
          "1,1,1,1|2,2,1,1|3,3,3,2|4,1,1,1");
    check("navigation",
          "SELECT id,lag(v,1) OVER(PARTITION BY grp ORDER BY id), "
          "lead(v,1) OVER(PARTITION BY grp ORDER BY id) FROM inventory_values ORDER BY id",
          "1,NULL,10|2,10,30|3,10,NULL|4,NULL,NULL");
    check("frames",
          "SELECT id,first_value(v) OVER(PARTITION BY grp ORDER BY id), "
          "last_value(v) OVER(PARTITION BY grp ORDER BY v), "
          "last_value(v) OVER(PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND "
          "UNBOUNDED FOLLOWING), "
          "nth_value(v,3) OVER(PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND "
          "UNBOUNDED FOLLOWING) "
          "FROM inventory_values ORDER BY id",
          "1,10,10,30,30|2,10,10,30,30|3,10,30,30,30|4,NULL,NULL,NULL,NULL");
    check("running_rows",
          "SELECT id,sum(v) OVER(PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING "
          "AND CURRENT ROW) FROM inventory_values ORDER BY id",
          "1,10|2,20|3,50|4,NULL");
    capability("filter", "SELECT count(*) FILTER(WHERE v>10) FROM inventory_values",
               postgres || sqlite, "1");
    capability("distinct_on",
               "SELECT DISTINCT ON(grp) id FROM inventory_values ORDER BY grp,id DESC", postgres,
               "3|4");
    capability("window_distinct",
               "SELECT count(DISTINCT v) OVER(PARTITION BY grp) FROM inventory_values", false, "");
    check("union",
          "SELECT count(*) FROM (SELECT v FROM inventory_values UNION SELECT v FROM "
          "inventory_other WHERE id IN(1,3,4)) combined",
          "3");
    check("union_all",
          "SELECT count(*) FROM (SELECT v FROM inventory_values UNION ALL SELECT v FROM "
          "inventory_other WHERE id IN(1,3,4)) combined",
          "7");
    check("intersect",
          "SELECT count(*) FROM (SELECT v FROM inventory_values INTERSECT SELECT v FROM "
          "inventory_other WHERE id IN(1,3,4)) combined",
          "3");
    check("except",
          "SELECT count(*) FROM (SELECT v FROM inventory_values EXCEPT SELECT v FROM "
          "inventory_other WHERE id IN(1,3,4)) combined",
          "0");
    capability("intersect_all",
               "SELECT count(*) FROM (SELECT v FROM inventory_values INTERSECT ALL SELECT v FROM "
               "inventory_other WHERE id IN(1,3,4)) combined",
               !sqlite, "3");
    capability("except_all",
               "SELECT count(*) FROM (SELECT v FROM inventory_values EXCEPT ALL SELECT v FROM "
               "inventory_other WHERE id IN(1,3,4)) combined",
               !sqlite, "1");
    check(
        "precedence",
        "SELECT count(*) FROM (SELECT 1 AS v UNION SELECT 2 AS v INTERSECT SELECT 2 AS v) combined",
        sqlite ? "1" : "2");
    capability("parenthesized_arms",
               "SELECT count(*) FROM ((SELECT id FROM inventory_values ORDER BY id LIMIT 1) "
               "UNION ALL (SELECT id FROM inventory_other ORDER BY id DESC LIMIT 1)) combined",
               !sqlite, "2");
    check("derived_arms",
          "SELECT count(*) FROM (SELECT * FROM (SELECT id FROM inventory_values ORDER BY id LIMIT "
          "1) left_arm "
          "UNION ALL SELECT * FROM (SELECT id FROM inventory_other ORDER BY id DESC LIMIT 1) "
          "right_arm) combined",
          "2");
    capability("offset_only", "SELECT id FROM inventory_values ORDER BY id OFFSET 1", postgres,
               "2|3|4");
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
