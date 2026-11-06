# Errors and value types

## Result and error model

`asterorm::result<T>` is `std::expected<T, db_error>`. Check the result before
accessing its value. Propagate a failure with `std::unexpected(result.error())`.

`db_error` contains `kind`, `sqlstate`, `message`, and optional `detail`, `hint`,
`table`, `column`, and `constraint`. Backend diagnostics populate these fields
where available; do not assume every field is present.

| `db_error_kind` | Typical meaning |
| --- | --- |
| `connection_failed` | Connection establishment or pool startup failed |
| `query_failed` | SQL execution or an operation contract failed; includes missing `find()` rows |
| `parse_failed` | Encoding, decoding, or mapping validation failed |
| `constraint_violation` | Database integrity constraint failed |
| `serialization_failure` | Transaction serialization conflict; retry helper can retry |
| `deadlock_detected` | Deadlock; retry helper can retry |
| `stale_write` | Optimistic-lock version mismatch |
| `timeout` | Timed operation failed, such as waiting for a pool lease |
| `unknown` | Unclassified failure |

`classify_sqlstate()` classifies known SQLSTATE codes, returns `query_failed`
for unrecognized nonempty codes, and `unknown` for empty input. MySQL native
error numbers appear in diagnostic detail alongside SQLSTATE.

## Shared codecs

Declarations: [codecs.hpp](../../include/asterorm/core/codecs.hpp) and
[types.hpp](../../include/asterorm/core/types.hpp).

| C++ value | Representation or purpose |
| --- | --- |
| Integer and floating types | Text numeric encoding with checked decoding |
| `bool` | Boolean encoding; SQLite binds integer 1/0 |
| `std::string` | Text |
| `std::optional<T>` | SQL NULL or encoded `T` |
| `std::vector<T>` | PostgreSQL array text; nullable elements use optional `T` |
| `asterorm::jsonb` | JSONB text in `.value` |
| `asterorm::uuid` | Sixteen bytes in `.bytes`, canonical UUID text |
| `asterorm::bytea` | Bytes in `.data`, PostgreSQL hexadecimal bytea text |
| `std::chrono::system_clock::time_point` | Timestamp codec |
| `asterorm::date` | Year, month, day |
| `asterorm::time_of_day` | Hour, minute, second, microsecond |
| `asterorm::numeric` | Exact decimal text in `.value` |

Codec availability does not imply a native database type on every backend.
Arrays, JSONB, and bytea are PostgreSQL-specific representations; see
[backend capabilities](backends.md).

For enum label mapping, specialize `asterorm::enum_traits<YourEnum>` with
`static std::string_view to_db(YourEnum)` and
`static std::optional<YourEnum> from_db(std::string_view)`. The decoder uses
an empty optional to reject an unknown label.

Encoded parameters distinguish an encoding error from a valid SQL NULL. Result
objects own their returned rows; views obtained from a backend result must not
outlive that result.
