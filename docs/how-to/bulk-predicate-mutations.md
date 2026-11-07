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
bound literals for user input.

For a mapped numeric `price` column, apply an adjustment in the database:

```cpp
namespace sql = asterorm::sql;
asterorm::repository repo{db};
auto repriced = repo.update_where<Job>(
    sql::col("cutoff") < sql::val(1700),
    {{"price", sql::multiply(sql::add(sql::col("price"), sql::val(5)), sql::val(2))}});
if (!repriced) return std::unexpected(repriced.error());
```

`sql::add(left, right)` and `sql::multiply(left, right)` accept runtime columns,
encoded values, or nested arithmetic. Every operation is parenthesized; values
are bound in left-to-right traversal order, followed by predicate parameters.
The example emits `price = ((price + $1) * $2)` on PostgreSQL/SQLite, with quoted
identifiers, or `?` placeholders on MySQL. The C++ helpers perform no arithmetic.

Every column reference is validated against the mapping before leasing. Primary
keys and generated columns can be read as operands; existing write protections
apply to destinations. Functions and raw fragments cannot be arithmetic operands.
Factory nodes are immutable and own their encoded values. Null nodes, invalid
operators, encoded-value errors, more than 64 nested operations, or more than
1024 operand/node visits per expression reject before acquisition. Reused subtrees
count each visit; this also bounds malformed cyclic/custom graphs.

Arithmetic uses native SQL types, casts, rounding, overflow and constraints.
Nullable arithmetic propagates SQL NULL, and a NOT NULL destination can reject
the write. This runtime API does not add compile-time numeric type checking;
text or boolean operands may be rejected or coerced differently by backends.
Parameter-only expressions may need server type information that this API does
not supply. There is no implicit SQL cast or C++ overflow policy.

Assignment list order is preserved. If a later assignment reads a column changed
earlier in the same list, MySQL can use its new value, while PostgreSQL and SQLite
read old row values. For portable adjustments, update one target at a time or
base expressions on columns whose values do not change in that statement.
See [MySQL UPDATE](https://dev.mysql.com/doc/refman/8.4/en/update.html),
[PostgreSQL UPDATE](https://www.postgresql.org/docs/16/sql-update.html), and
[SQLite UPDATE](https://www.sqlite.org/lang_update.html).

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

The arithmetic alternative in `sql::value_source` also changes its public
variant contract. Rebuild library and consumers together and update exhaustive
visitors for `arithmetic_ptr`. `expr_ast`, SELECT and predicate expression shapes
are unchanged; the helpers extend assignment values only.

## Insert rows from a SELECT

`repo.insert_select<T>(sql::select_ast source)` returns a checked affected-row
count after one observed prepared statement. It does not load source rows into
C++, refresh entities, or implicitly start/commit a transaction. Zero copied rows
is success.

For a mapped `ArchivedJob` whose column order is generated `archive_id`,
`source_id`, `price`, and nullable `note`, copy eligible jobs by position:

```cpp
namespace sql = asterorm::sql;
asterorm::repository repo{db};
auto source = sql::select({"id", "price", "note"})
    .from("jobs")
    .where(sql::col("cutoff") < sql::val(1700))
    .order_by("id")
    .build();
auto archived = repo.insert_select<ArchivedJob>(std::move(source));
if (!archived) return std::unexpected(archived.error());
```

The destination table comes from `entity_traits<T>`. Destination columns follow
mapping order, omitting both `generated::always` and `generated::by_default`
(including non-key defaults). Non-generated keys are included. At least one
destination is required. Source aliases do not match fields by name: projection
positions must match this inferred destination order. A non-generated version
column is supplied explicitly by the projection; insertion performs no optimistic
update check or automatic version increment. Generated ID values are not returned
and sequences/counters can have gaps after errors or rollback.

The v1 source has one table and an explicit, matching-width column projection.
Use either `columns` or column-only `items`; mixing them rejects. Table/column
aliases, WHERE comparisons/AND/OR/NULL tests, ORDER BY, LIMIT and OFFSET are
supported. OFFSET requires LIMIT. Wildcards, joins, functions (including WHERE
functions), grouping/HAVING and row locking reject before acquisition. Malformed
predicates reject too; source predicates are limited to depth 64 and 1024 nodes.
There is no conflict handling, RETURNING, custom destination subset, or explicit
generated-key insertion overload.

Source schema existence and SQL type compatibility remain server responsibilities;
runtime AST shape checking does not compare source fields with C++ target types.
Encoded parameter failures retain their diagnostic and reject locally. Other
server errors and observer exceptions follow the mutation rules above. Typed
SELECT builders can be consumed through `.build()`; their typed source alone
does not prove compatibility with the target.

On MySQL, equal final source/destination table-name components, ignoring ASCII
case, reject even when their schema prefixes differ. This conservative v1 guard
does not resolve views, server aliases or Unicode normalization. It is an API
restriction; some [native MySQL forms](https://dev.mysql.com/doc/refman/8.4/en/insert-select.html)
can select from their destination. Other server name-resolution rules remain in
force. ClickHouse and unknown capabilities reject before leasing.

Low-level callers can construct `sql::insert_select_ast{table, columns, source}`
and use `compiler::compile`; the same shape/dialect checks apply there. The source
uses the existing SELECT compiler and its single parameter vector. Existing
`insert_ast` remains VALUES-only and its layout is unchanged.

See the [design and remaining deliveries](../bulk-predicate-dml-design.md).
