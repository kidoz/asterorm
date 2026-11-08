#include "asterorm/core/codecs.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/sql/compiler.hpp"
#include <cstdlib>
#include <iostream>
#include <stdexcept>

namespace {
void emit(std::string_view name, std::string_view value) {
    constexpr std::string_view hex = "0123456789abcdef";
    std::cout << name << '\t';
    for (unsigned char character : value) {
        std::cout << hex[character / 16U] << hex[character % 16U];
    }
    std::cout << '\n';
}
void checked(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string{message});
    }
}
std::string identifier(std::string_view name) {
    std::string quoted{"\""};
    for (char character : name) {
        if (character == '"') {
            quoted += '"';
        }
        quoted += character;
    }
    return quoted + '"';
}
struct rollback_guard {
    asterorm::pg::connection& connection;
    bool active{true};
    ~rollback_guard() {
        if (active) {
            try {
                if (!connection.execute("ROLLBACK")) {
                    std::fputs("type inventory rollback failed\n", stderr);
                }
            } catch (...) {
                std::fputs("type inventory rollback threw\n", stderr);
            }
        }
    }
};
void command(asterorm::pg::connection& connection, std::string_view sql) {
    auto outcome = connection.execute(sql);
    if (!outcome) {
        throw std::runtime_error("Inventory fixture command failed: " + outcome.error().message);
    }
}
void observe(asterorm::pg::connection& connection, const std::string& name, std::string_view sql,
             const asterorm::query_parameters& parameters = {}) {
    command(connection, "SAVEPOINT type_probe");
    auto result = connection.execute_params(sql, parameters);
    if (!result) {
        emit(name + ".sqlstate", result.error().sqlstate);
        emit(name + ".error", result.error().message);
        command(connection, "ROLLBACK TO SAVEPOINT type_probe");
    } else {
        emit(name + ".rows", std::to_string(result->rows()));
        for (int row = 0; row < result->rows(); ++row) {
            for (int column = 0; column < result->columns(); ++column) {
                auto text = result->get_string(row, column);
                const auto field = name + "." + std::to_string(row) + "." + std::to_string(column);
                emit(field + ".null", text ? "false" : "true");
                if (text) {
                    emit(field, *text);
                }
            }
        }
    }
    command(connection, "RELEASE SAVEPOINT type_probe");
}
void codec_inventory() {
    auto nested = asterorm::encode_parameter(std::vector<std::vector<int>>{{1, 2}, {3, 4}});
    emit("codec.nested.encode", nested.value.value_or("<NULL>"));
    std::vector<std::vector<int>> nested_out;
    auto nested_decoded = asterorm::decode(nested.value, nested_out);
    emit("codec.nested.decode_success", nested_decoded ? "true" : "false");
    std::vector<int> numbers{99};
    auto lower = asterorm::decode(std::optional<std::string>{"[0:1]={1,2}"}, numbers);
    emit("codec.array_bounds.decode_success", lower ? "true" : "false");
    numbers = {99};
    auto malformed = asterorm::decode(std::optional<std::string>{"{1,2junk}"}, numbers);
    emit("codec.array_malformed.decode_success", malformed ? "true" : "false");
    emit("codec.array_malformed.output_size", std::to_string(numbers.size()));
    emit("codec.array_malformed.first",
         numbers.empty() ? "<empty>" : std::to_string(numbers.front()));
    auto json_array = asterorm::encode_parameter(std::vector<asterorm::jsonb>{{"{\"a\":1}"}});
    emit("codec.json_array.encode", json_array.value.value_or("<NULL>"));
    asterorm::jsonb unchecked;
    auto json_decoded = asterorm::decode(std::optional<std::string>{"not JSON"}, unchecked);
    emit("codec.json_unvalidated", json_decoded ? unchecked.value : "rejected");
    std::chrono::system_clock::time_point timestamp;
    auto invalid_time =
        asterorm::decode(std::optional<std::string>{"2026-02-30 25:99:00+00junk"}, timestamp);
    emit("codec.timestamp_malformed.accepted", invalid_time ? "true" : "false");
    if (invalid_time) {
        emit("codec.timestamp_malformed.normalized",
             asterorm::encode(timestamp).value_or("<NULL>"));
    }
    asterorm::numeric special;
    auto special_decoded = asterorm::decode(std::optional<std::string>{"NaN"}, special);
    emit("codec.numeric_nan.decode_success", special_decoded ? "true" : "false");
    auto bad_numeric = asterorm::encode_parameter(asterorm::numeric{"not numeric"});
    emit("codec.numeric_invalid.encode_error", bad_numeric.error ? "true" : "false");
    std::chrono::system_clock::time_point local_instant;
    std::chrono::system_clock::time_point utc_instant;
    auto local_result =
        asterorm::decode(std::optional<std::string>{"1930-01-01 00:00:00+00:19:32"}, local_instant);
    auto utc_result =
        asterorm::decode(std::optional<std::string>{"1929-12-31 23:40:28+00"}, utc_instant);
    emit("codec.timestamp_offset_seconds.equal",
         local_result && utc_result && local_instant == utc_instant ? "true" : "false");
    asterorm::sql::compiler sqlite{asterorm::sql::dialect::sqlite};
    auto expression = asterorm::sql::select({})
                          .from("documents")
                          .items({
                              asterorm::sql::select_item{
                                  .fn = asterorm::sql::func("jsonb_path_exists",
                                                            {
                                                                asterorm::sql::col("document"),
                                                                asterorm::sql::val("$.a"),
                                                            }),
                              },
                          })
                          .build();
    auto compiled = sqlite.compile(expression);
    emit("ast.unchecked_json_function.sqlite_compiles", compiled ? "true" : "false");
    if (compiled) {
        emit("ast.unchecked_json_function.sql", compiled->sql);
    }
}
void native_inventory() {
    const char* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    auto connected = asterorm::pg::driver{}.connect(
        configured ? configured
                   : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test "
                     "sslmode=disable");
    checked(connected.has_value(), "PostgreSQL inventory connection unavailable");
    auto& connection = *connected;
    auto extensions_before =
        connection.execute("SELECT count(*) FROM pg_extension WHERE extname='hstore'");
    checked(extensions_before.has_value(), "Cannot inspect extension state");
    const auto original_extension_count = extensions_before->get_string(0, 0);
    command(connection, "BEGIN");
    rollback_guard rollback{.connection = connection};
    const auto schema = "asterorm_type_inventory_" +
                        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto quoted_schema = identifier(schema);
    command(connection, "CREATE SCHEMA " + quoted_schema);
    auto installed = connection.execute("SELECT n.nspname FROM pg_extension e JOIN pg_namespace n "
                                        "ON n.oid=e.extnamespace WHERE e.extname='hstore'");
    checked(installed.has_value(), "Cannot inspect hstore namespace");
    std::string hstore_schema = schema;
    if (installed->rows()) {
        hstore_schema = installed->get_string(0, 0).value();
    } else {
        command(connection, "CREATE EXTENSION hstore WITH SCHEMA " + quoted_schema);
    }
    command(connection, "SET LOCAL search_path TO pg_catalog," + quoted_schema + "," +
                            identifier(hstore_schema));
    command(connection, "SET LOCAL DateStyle TO 'ISO, YMD'");
    command(connection, "SET LOCAL TimeZone TO 'UTC'");
    observe(connection, "server",
            "SELECT "
            "current_setting('server_version'),current_setting('DateStyle'),current_setting('"
            "TimeZone')");
    observe(connection, "range.discrete",
            "SELECT '[1,3]'::int4range::text, '(1,2)'::int4range::text, "
            "'[2024-02-28,2024-02-29]'::daterange::text, '[1.00,2.50]'::numrange::text");
    observe(connection, "range.null_empty_unbounded",
            "SELECT NULL::int4range, 'empty'::int4range::text, '(,)'::int4range::text, "
            "lower_inf('(,)'::int4range), lower_inc('(,)'::int4range)");
    observe(connection, "range.infinities",
            "SELECT '[infinity,infinity]'::tstzrange::text, "
            "lower_inf('[infinity,infinity]'::tstzrange), '(,)'::tstzrange::text");
    observe(connection, "range.reverse", "SELECT '[3,1]'::int4range");
    observe(connection, "range.overflow", "SELECT '[2147483647,2147483647]'::int4range");
    observe(connection, "range.numeric_special", "SELECT '[NaN,NaN]'::numrange::text");
    command(connection, "SET LOCAL TimeZone TO 'Pacific/Chatham'");
    observe(connection, "range.timezone",
            "SELECT '[2026-01-01 00:00:00+00,2026-01-01 01:00:00+00)'::tstzrange::text");
    observe(connection, "interval.calendar",
            "SELECT '1 mon'::interval='30 days'::interval, (date '2024-01-31' + interval '1 "
            "mon')::date::text, (date '2024-01-31' + interval '30 days')::date::text");
    command(connection, "SET LOCAL TimeZone TO 'UTC'");
    for (const char* style : {"postgres", "postgres_verbose", "sql_standard", "iso_8601"}) {
        command(connection, "SET LOCAL IntervalStyle TO '" + std::string{style} + "'");
        observe(connection, std::string{"interval."} + style,
                "SELECT '1 year 2 mons -3 days 04:05:06.000007'::interval::text, EXTRACT(year FROM "
                "'1 year 2 mons -3 days 04:05:06.000007'::interval)*12+EXTRACT(month FROM '1 year "
                "2 mons -3 days 04:05:06.000007'::interval)");
    }
    command(connection, "SET LOCAL IntervalStyle TO 'postgres'");
    observe(connection, "network",
            "SELECT "
            "'192.0.2.9/24'::inet::text,'192.0.2.0/24'::cidr::text,'2001:db8::1/"
            "64'::inet::text,'08002B:010203'::macaddr::text");
    observe(connection, "network.cidr_hostbits", "SELECT '192.0.2.9/24'::cidr");
    observe(connection, "array.native",
            "SELECT '{{1,2},{3,4}}'::integer[]::text,'[0:1]={1,2}'::integer[]::text");
    observe(connection, "array.json_codec", "SELECT $1::jsonb[]",
            {asterorm::encode_parameter(std::vector<asterorm::jsonb>{{"{\"a\":1}"}})});
    command(connection, "CREATE TYPE " + quoted_schema +
                            ".inner_record AS (label text, note text, number integer)");
    command(connection, "CREATE TYPE " + quoted_schema + ".outer_record AS (inside " +
                            quoted_schema + ".inner_record, label text)");
    observe(connection, "composite.null_empty",
            "SELECT NULL::inner_record, ROW(NULL::text,NULL::text,NULL::integer)::inner_record, "
            "ROW(''::text,NULL::text,7)::inner_record");
    observe(connection, "composite.nested",
            "SELECT ROW(ROW($1::text,''::text,7)::inner_record,$2::text)::outer_record",
            {"comma,quote\"slash\\", "outer"});
    observe(connection, "hstore",
            "SELECT hstore(ARRAY['literal','nil','empty'], ARRAY['NULL',NULL,''])::text, "
            "hstore(ARRAY['q'], ARRAY[$1::text])::text",
            {"comma,quote\"slash\\"});
    observe(connection, "json.null_missing",
            "SELECT $1::jsonb->'nil', $1::jsonb->>'nil', $1::jsonb->'absent', $1::jsonb->>'absent'",
            {"{\"nil\":null}"});
    observe(connection, "json.path_exists",
            "SELECT jsonb_path_exists($1::jsonb,$2::jsonpath,$3::jsonb,false)",
            {"{\"a\":[1,2]}", "$.a[*] ? (@ > $min)", "{\"min\":1}"});
    observe(connection, "json.path_rows", "SELECT jsonb_path_query($1::jsonb,$2::jsonpath)",
            {"{\"a\":[1,2]}", "$.a[*]"});
    observe(connection, "json.invalid_path", "SELECT jsonb_path_exists('{}'::jsonb,$1::jsonpath)",
            {"$.a["});
    observe(connection, "index.opclasses",
            "SELECT t.typname,string_agg(a.amname||':'||c.opcname||':'||c.opcdefault::text,',' "
            "ORDER BY a.amname,c.opcname) FROM pg_opclass c JOIN pg_type t ON t.oid=c.opcintype "
            "JOIN pg_am a ON a.oid=c.opcmethod JOIN pg_namespace n ON n.oid=c.opcnamespace WHERE "
            "(n.nspname='pg_catalog' OR n.nspname=$1) AND t.typname IN "
            "('anyrange','inet','cidr','interval','macaddr','hstore','jsonb','record','anyarray') "
            "GROUP BY t.typname ORDER BY t.typname",
            {hstore_schema});
    command(connection, "CREATE TEMP TABLE type_index_probe(r int4range,doc jsonb,n inet,i "
                        "interval,m macaddr,h hstore)");
    command(connection, "INSERT INTO type_index_probe VALUES('[1,4)','{\"a\":1}','192.0.2.9/24','1 "
                        "mon','08:00:2b:01:02:03','a=>1')");
    for (const auto* statement : {
             "CREATE INDEX range_idx ON type_index_probe USING gist(r)",
             "CREATE INDEX json_idx ON type_index_probe USING gin(doc)",
             "CREATE INDEX inet_idx ON type_index_probe USING gist(n inet_ops)",
             "CREATE INDEX interval_idx ON type_index_probe(i)",
             "CREATE INDEX mac_idx ON type_index_probe(m)",
             "CREATE INDEX hstore_idx ON type_index_probe USING gin(h)",
         }) {
        command(connection, statement);
    }
    command(connection, "SET LOCAL enable_seqscan TO off");
    for (const auto& [name, predicate] : std::vector<std::pair<std::string, std::string>>{
             {"range", "r && '[2,3)'::int4range"},
             {"json", "doc @> '{\"a\":1}'::jsonb"},
             {"inet", "n <<= '192.0.2.0/24'::inet"},
             {"interval", "i='1 mon'::interval"},
             {"mac", "m='08:00:2b:01:02:03'::macaddr"},
             {"hstore", "h @> 'a=>1'::hstore"},
         }) {
        observe(connection, "index.plan." + name,
                "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT * FROM type_index_probe WHERE " +
                    predicate);
    }
    command(connection, "ROLLBACK");
    rollback.active = false;
    auto cleanup = connection.execute_params("SELECT to_regnamespace($1) IS NULL,(SELECT count(*) "
                                             "FROM pg_extension WHERE extname='hstore')::text",
                                             {schema});
    checked(cleanup.has_value() && cleanup->get_string(0, 0) == "t" &&
                cleanup->get_string(0, 1) == original_extension_count,
            "Inventory rollback did not restore schema/extension state");
    emit("cleanup.restored", "true");
}
} // namespace
int main() {
    try {
        codec_inventory();
        native_inventory();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
