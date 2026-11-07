# Update and delete rows by predicate

`repository::update_where<T>(predicate, assignments)` and
`delete_where<T>(predicate)` execute one prepared statement and return
`result<std::size_t>`. Zero affected rows is success. They are available on
PostgreSQL 16+, SQLite 3.37+, and MySQL 8.0.19+ (the test service uses 8.4).

For a mapped `Job` with `id`, `cutoff`, `active`, and nullable `note` columns,
and an existing session `db`:

```cpp
namespace sql = asterorm::sql;
asterorm::repository repo{db};
auto marked = repo.update_where<Job>(
    sql::col("cutoff") < sql::val(1700),
    {{"active", sql::val(false)}});
if (!marked) return std::unexpected(marked.error());

auto removed = repo.delete_where<Job>(
    sql::is_null(sql::col("note")) && sql::col("cutoff") < sql::val(1000));
if (!removed) return std::unexpected(removed.error());
```

The table comes from `entity_traits<T>`. The required predicate supports mapped
bare columns, encoded parameters, comparisons, AND/OR, and IS NULL/IS NOT NULL.
Functions and qualified column names reject before acquiring a connection. A
required predicate can still be a tautology; choose it deliberately.

Assignments use mapped bare column names. Empty lists, duplicates, unknown names,
primary keys (including composite keys), and `generated::always` columns reject
locally. Non-key `generated::by_default` columns are writable. Tables without a
mapped primary key are supported. Versioned entities reject both methods in this
first slice; no implicit version increment or optimistic check is performed.

Literal values are bound, including boolean type metadata. To assign NULL, use
`sql::val(std::optional<std::string>{})` for a nullable text field. Encoding errors
retain their codec diagnostic and reject locally. Explicit `sql::raw_expr`
assignment values remain caller-owned SQL; embedded NUL fragments reject. Use
bound literals for user input. Structured bound arithmetic and
`insert_select` remain planned.

Pass a typed predicate by explicitly moving its `inner` member into the method.
This lowers to the same runtime AST; it does not add typed assignment validation.
There are no UPDATE/DELETE joins, LIMIT, RETURNING, or entity refresh operations.

The methods use `with_connection` and `observed_execute` once. Existing managed
transactions and PostgreSQL pool/scoped timeouts apply. Use `transact` or an
explicit transaction when rollback is required. Without one, a successful write
can be committed before an observer throws or a count conversion reports an
error. Database errors retain their diagnostics; there is no automatic retry.

Counts describe the direct backend statement, with matched no-change updates
counted by the tested relational adapters. They exclude auxiliary trigger/cascade
work and are not a count of refreshed C++ objects. A zero match is not
`stale_write`. Invalid local inputs emit no query event; a dispatched statement
uses the normal single success/error event.

Custom sessions must expose a `connection_type` with an explicit relational
`sql_dialect` and `static constexpr bool supports_bulk_predicate_dml = true`, plus
compatible `with_connection` and `observed_execute` methods. Their result count
must be integral, nonnegative, and representable as `size_t`. Unknown capabilities
and ClickHouse reject before acquisition; ordinary existing APIs retain their
compatibility behavior.

Relational `result::affected_rows()` now returns `std::uint64_t`. Rebuild the
library and consumers together, including code depending on result layouts,
constructors, or exact accessor types. PostgreSQL's `result::from_native` takes
ownership on success/failure and returns structured status/count errors; its
legacy raw-handle constructor throws `bad_expected_access<db_error>` for an
invalid count and still releases the handle.

See the [design and remaining deliveries](../bulk-predicate-dml-design.md).
