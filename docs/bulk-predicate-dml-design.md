# Bulk predicate DML: inventory and implementation contract

Reviewed on 2026-11-07 against the working tree. The evidence records its Git base
and inspected source hashes. **Inventory complete; implementation remains
planned.** `update_where`, `delete_where`, and `insert_select` are absent.
This contract defines the next feature's boundaries and implementation sequence;
it does not establish API acceptance or release readiness.

## Current implementation

| Area | Observed code | Consequence |
| --- | --- | --- |
| Repository writes | [repository.hpp](../include/asterorm/core/repository.hpp) has entity/key writes, multi-row insert/upsert, and generated-value refresh | No set-based predicate mutation or INSERT-from-SELECT method; existing bulk insert/upsert is a different surface |
| Mutation AST | [ast.hpp](../include/asterorm/sql/ast.hpp) has UPDATE assignments, DELETE, optional WHERE and RETURNING | Reuse these nodes for predicate mutations; repository methods must require a predicate |
| Assignment sources | `value_source` is `param_expr` or explicit `raw_expr` | Literal assignments work; column arithmetic has no structured, bound-value representation |
| Compiler | [compiler.cpp](../src/sql/compiler.cpp) parameterizes assignments before predicates and emits PostgreSQL/SQLite `$N` or MySQL `?` placeholders | Reuse the single compiler; do not construct a second SQL generator in the repository |
| Validation | Empty UPDATE assignments still compile to incomplete SQL; the compiler has no entity metadata and accepts unmapped targets | Add repository mapping validation and structural checks before acquiring a lease |
| INSERT source | `insert_ast` requires nonempty `rows`; no SELECT source exists | Add an AST/compiler surface before exposing `insert_select` |
| Execution | [session.hpp](../include/asterorm/core/session.hpp) provides `with_connection` and `observed_execute` | One prepared mutation and one event per attempted execution; transaction pins and PostgreSQL timeouts apply to this path |
| Counts | All three relational result APIs return `int` from `affected_rows()` | A `size_t` cast in the repository cannot repair lost or rejected native counts |
| Optimistic locking | Entity updates compare/increment `version_column` and report `stale_write` | Bulk methods must define their own policy instead of silently bypassing that protection |
| Experimental backend | [ClickHouse results](../src/database/clickhouse/result.cpp) equate affected count with returned rows | Do not expose these bulk methods on ClickHouse under a relational affected-row contract |

The [reproducible probe](evidence/bulk-predicate-dml-inventory/README.md) verifies
the existing compiler and adapter execution path. It uses temporary tables and
native INSERT-from-SELECT SQL, not the proposed repository APIs. Its compile-time
checks also confirm the three proposed entry points are absent for the proposed
argument types.

## Count transport is a prerequisite

PostgreSQL currently parses `PQcmdTuples` into `int` and returns zero on parse
failure, including overflow. SQLite reads `sqlite3_changes`, whose result is
undefined above `INT_MAX`. MySQL detects oversized counts but returns an execution
error after the server may already have applied the write. These are source
findings; the probe does not execute billions of mutations.

Before publishing bulk affected-row results:

- Carry an unsigned 64-bit count through relational adapter results. Keep row and
  column indices separate; changing mutation count transport does not require
  changing hydration indices.
- Parse/check PostgreSQL command counts without narrowing; use SQLite's
  `sqlite3_changes64`; remove MySQL's artificial `int` storage limit. Validate
  conversions to repository `size_t`, including narrower platforms.
- Never clamp, wrap, or turn an unrepresentable/malformed count into successful
  zero. Report a structured error. Such an error may follow a completed write;
  only an explicit caller transaction and rollback can undo it.
- Prefer a consistent wide `affected_rows()` accessor across built-in relational
  results. This changes a public type/ABI: rebuild consumers and document custom
  result/facade compatibility. Finalize the concrete accessor/storage change in
  that implementation slice, with no-server boundary tests using synthetic
  command-count inputs.

PostgreSQL counts matched updates even when values do not change, and this MySQL
driver enables `CLIENT_FOUND_ROWS`. The live probe also confirms that SQLite
reports the same no-change count for its fixture. Counts describe the direct
statement's backend result, not the number of distinct new values, triggers,
cascades, or application objects. PostgreSQL triggers can suppress updates; the
API does not promise that a prior SELECT count will equal the mutation count.
See [PostgreSQL UPDATE](https://www.postgresql.org/docs/16/sql-update.html),
[SQLite change counts](https://www.sqlite.org/c3ref/changes.html), and
[MySQL connection flags](https://dev.mysql.com/doc/c-api/8.4/en/mysql-real-connect.html).

## First repository slice

The proposed runtime signatures are:

```cpp
template <typename T>
asterorm::result<std::size_t> update_where(
    asterorm::sql::predicate_ast predicate,
    std::vector<std::pair<std::string, asterorm::sql::value_source>> assignments);

template <typename T>
asterorm::result<std::size_t> delete_where(
    asterorm::sql::predicate_ast predicate);
```

These are member-template declarations for the future `repository<Session>`;
they are not available calls today. A descriptive assignment-list alias may be
added if it improves the public header. Keep the core header independent of the
heavy typed-query bridge. Callers can explicitly lower an existing
`typed_predicate<T>` through its `inner` member; a dedicated typed-assignment
facade is a later convenience, not a prerequisite for this runtime API.

The first slice includes checked counts, both methods, and these rules:

1. Take the table from `entity_traits<T>`. Require at least one assignment for
   UPDATE. Reject duplicate, unknown, empty, NUL-containing, wildcard, and
   qualified assignment targets. Initial repository assignments use bare mapped
   column names; mappings with dots in writable names are outside this slice.
2. Reject primary-key and `generated::always` assignments. Ordinary non-key
   `generated::by_default` columns remain explicitly writable, consistent with
   entity updates. Do not fetch generated values or refresh in-memory entities.
3. Initially reject UPDATE/DELETE for entities declaring `version_column` before
   acquisition. A later explicit policy must settle per-row version increments
   and expectations; counts alone cannot reproduce entity optimistic locking.
   Do not pretend a bulk zero-match is `stale_write`.
4. Require a valid predicate object. Initially accept mapped bare-column and
   parameter operands, comparisons, AND/OR, and IS NULL/IS NOT NULL. Reject
   functions, qualified references, invalid operator categories, and null child
   pointers before SQL execution. Runtime AST values remain codec/server checked;
   this does not create compile-time field/value type checking.
5. A required predicate is not proof of selectivity: a caller can deliberately
   supply a tautology. There is no omitted-WHERE overload, implicit primary-key
   filter, UPDATE/DELETE LIMIT, JOIN, or RETURNING in this repository slice.
6. Literal values use `encode_parameter` and retain boolean/null metadata. Check
   stored parameter-encoding errors before acquisition. Preserve deterministic
   assignment order followed by predicate traversal order.
7. Preserve the existing explicit `raw_expr` escape hatch on assignment values:
   caller-owned SQL, never auto-created from ordinary strings, with embedded NUL
   rejection. It does not provide type checking or parameterize its fragment.
8. Validation failures use `parse_failed`; unsupported backends/version policy
   use `query_failed`, without fabricated SQLSTATEs. No lease, observer event, or
   mutation occurs for these local rejections.

Execution compiles once, then calls `with_connection` and `observed_execute`
exactly once, and returns the checked count. Zero is success. There is no entity
loop, SELECT-before-write, hidden transaction, refresh SELECT, or automatic retry.
An existing transaction remains caller-owned; a failed PostgreSQL statement may
require rollback. Observer exceptions retain existing session behavior: they may
occur after the statement executes and do not imply automatic rollback.
PostgreSQL pool/scoped timeouts apply to the server statement, not to validation,
pool wait, the callback, or an end-to-end deadline. MySQL and SQLite retain their
existing backend timeout behavior.

Target the existing supported [backend baselines](reference/backends.md):
PostgreSQL 16+, SQLite 3.35+ in the project's current support surface, and MySQL
8.0.19+ (local/CI service 8.4). New bulk repository methods must explicitly reject
ClickHouse and unknown custom connection capabilities before acquisition; a
PostgreSQL-compatible dialect fallback alone does not prove mutation support.

## Bound arithmetic and INSERT-from-SELECT

After the first slice, add structured arithmetic assignment values for at least
addition and multiplication. A bulk price update must be representable with a
quoted mapped column and bound adjustment, for example `price = price * $1`.
Extend the existing expression/value AST and compiler; do not concatenate user
values into `raw_expr`. Preserve existing literal/raw construction and test
placeholder order, parenthesization, null behavior, and type/SQL errors. Native
SQL remains an explicit escape hatch while this helper is unimplemented.

The proposed INSERT entry point is `insert_select<T>(sql::select_ast source)`
returning `result<size_t>`. Introduce a dedicated `insert_select_ast` with target
table, destination columns, and SELECT source. Keeping `insert_ast` rows-only
avoids ambiguous simultaneous VALUES/SELECT states. The compiler owns the prefix
and source rendering, retaining one parameter vector and statement.

For its first implementation:

- Infer destination columns from `entity_traits<T>::columns` in mapping order,
  excluding `generated::always` and `generated::by_default`. Non-generated keys
  are included. Reject an empty inferred destination. Explicit generated-key
  insertion and custom destination subsets need a later overload.
- Require an explicit column-only source projection with matching arity. No `*`,
  joins, functions, grouping/HAVING, row-lock clauses, conflicts, or RETURNING in
  this slice. Table aliases and column aliases may be retained. ORDER BY,
  LIMIT, and OFFSET are allowed, but OFFSET requires LIMIT for the initial
  portable surface. Reject invalid source shape before acquisition.
- Source and destination may be different entity tables. Runtime AST arity is
  checked locally; SQL type compatibility, constraints, defaults, and source
  schema existence remain server responsibilities. No DTO hydration is involved.
- Reject a MySQL source referring to its destination table before acquisition
  in the portable v1 surface; do not rely on vendor materialization behavior.
  This is a deliberate API limit, not a claim that every such native query fails.
- Execute once on the same session path and return a checked count. No readback,
  client-side row transfer, implicit commit, or automatic retry. A versioned
  target's non-generated version field is an explicit projected value, with no
  optimistic-update check implied by insertion.

Typed SELECT builders can later lower through `build()` into this source. Any
typed convenience wrapper must separately check destination compatibility; a
typed source alone does not establish compatible target fields.

## Acceptance and delivery sequence

| Delivery | Required evidence |
| --- | --- |
| Checked counts and literal UPDATE/DELETE | No-server count boundaries (0, INT_MAX, INT_MAX+1, wide limit, malformed/negative input and size conversion); spy-session SQL/parameter/count/error checks; validation without acquisition; zero-match success; generated/composite-key/version policy cases |
| Shared mutation execution | PostgreSQL/SQLite/MySQL fixtures for soft-flag flip, archive-after-cutoff deletion, no-change/zero-match results, uniqueness failure and caller rollback; one success/error observer event per attempted statement; no event for preflight rejection |
| Structured price changes | Compiler and no-server API tests for bound arithmetic; live bulk price update with untouched-row checks and rollback; primary-key/generated-column protections still apply |
| INSERT-from-SELECT | Compiler shape/parameter tests and no-server rejection cases; live archive copy, empty source, generated defaults, projection mismatch, duplicate/constraint failure, rollback, observer counts, and MySQL self-source rejection |
| Timeout/capability integration | PostgreSQL delayed UPDATE/DELETE/INSERT-from-SELECT under pool/scoped timeout: SQLSTATE 57014 and explicit rollback recovery; disabled/custom/ClickHouse rejection before leasing, plus unchanged ordinary queries |
| Combined acceptance | Updated README inventory, guides that compile, backend limits and public count/AST ABI notes; fresh unit/integration, normal build, format and lint checks, with separate standards findings |

**Next implementation:** checked affected-row transport together with validated
literal `update_where` and `delete_where`. Keep bound arithmetic and
INSERT-from-SELECT as subsequent deliveries; the feature remains planned until
code exists and is partial until its required methods and acceptance checks close.

## Observed verification for this inventory

- `python3 docs/evidence/bulk-predicate-dml-inventory/capture.py`: compile exit 0,
  probe exit 0. Three dialects compiled. Each live backend reported UPDATE 2,
  unchanged UPDATE 2, no-match 0, DELETE 1, native INSERT-from-SELECT 1, six
  observer events with one error, and rollback restoring both temporary tables.
- `just test`: `Ok: 12`, `Fail: 0`, against the unchanged runtime tree.
- `just format-check` and explicit `clang-format --dry-run --Werror` on the probe:
  exit 0. The first format attempt caught an unformatted probe edit; the final
  formatted source was recaptured and checked.
- `clang-tidy` on the probe using its captured Meson compiler arguments and the
  local SDK: exit 0, no probe diagnostics. The standard lint target selects the
  unchanged build compilation database; this standalone evidence file was checked
  separately. Full project lint was not repeated for this documentation review.
- Capture-script syntax, relative documentation targets, and all four evidence
  plus ten inspected-source hashes: pass.
- Strict standards validation using `--mode strict-governance --workspace
  --today 2026-11-07`: exit 1 with the existing 53 errors and 12 warnings, no
  findings on the three edited context documents. These are not waived.

This is a local baseline and implementation contract. No new repository API,
large-count workload, ClickHouse runtime behavior, remote CI, or release result
is claimed. The locking/timeout evidence remains a historical snapshot and is
not rewritten by this review.
