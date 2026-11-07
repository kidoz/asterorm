# Bulk predicate DML: inventory and implementation contract

Reviewed on 2026-11-07. **Partially implemented:** checked 64-bit affected-row
transport, literal `update_where`, and `delete_where` are implemented under the
bounded contract below. Structured assignment arithmetic is also implemented;
`insert_select` is implemented with the source-shape and destination limits below;
combined feature acceptance remains pending.
See the [mutation guide](how-to/bulk-predicate-mutations.md) and implementation
verification below. This does not establish acceptance of the whole feature or
release readiness.

## Inventory before implementation

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

This table records the initial inventory, before the implementation below.
The [reproducible probe](evidence/bulk-predicate-dml-inventory/README.md) verifies
the existing compiler and adapter execution path. It uses temporary tables and
native INSERT-from-SELECT SQL, not the proposed repository APIs. Its compile-time
checks also confirm the three proposed entry points are absent for the proposed
argument types.

## Count transport prerequisite: implemented

At inventory time PostgreSQL parsed `PQcmdTuples` into `int` and returns zero on parse
failure, including overflow. SQLite read `sqlite3_changes`, whose result is
undefined above `INT_MAX`. MySQL detected oversized counts but returned an execution
error after the server may already have applied the write. These are source
findings; the probe does not execute billions of mutations.

The implementation now uses `uint64_t` storage/getters on relational results,
checked PostgreSQL count parsing through `result::from_native`, SQLite 3.37+
`sqlite3_changes64` / `sqlite3_total_changes64`, and MySQL's native unsigned count
without the former `int` cap. New bulk methods and `insert_many` check conversion
to `size_t`. Constructor/result layouts and exact getter types changed; rebuild
consumers. The legacy PostgreSQL raw-result constructor throws on invalid counts;
the execution adapter uses the factory to return structured errors instead.
The following requirements guided that implementation:

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

The implemented runtime signatures are:

```cpp
template <typename T>
asterorm::result<std::size_t> update_where(
    asterorm::sql::predicate_ast predicate,
    std::vector<std::pair<std::string, asterorm::sql::value_source>> assignments);

template <typename T>
asterorm::result<std::size_t> delete_where(
    asterorm::sql::predicate_ast predicate);
```

These are member templates on `repository<Session>`. Keep the core header independent of the
heavy typed-query bridge. Callers can explicitly lower an existing
`typed_predicate<T>` through its `inner` member; a dedicated typed-assignment
facade is a later convenience, not a prerequisite for this runtime API.

The first slice implements checked counts, both methods, and these rules:

1. Take the table from `entity_traits<T>`. Require at least one assignment for
   UPDATE. Reject duplicate, unknown, empty, NUL-containing, wildcard, and
   qualified assignment targets. Initial repository assignments use bare mapped
   column names; mappings with dots in writable names are outside this slice.
   Primary-key mappings are optional for these set-based operations.
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
8. Mapping/shape validation failures use `parse_failed`; codec diagnostics are
   preserved. Unsupported backends/version policy
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
PostgreSQL 16+, SQLite 3.37+ for the required 64-bit change-count functions, and MySQL
8.0.19+ (local/CI service 8.4). New bulk repository methods must explicitly reject
ClickHouse and unknown custom connection capabilities before acquisition; a
PostgreSQL-compatible dialect fallback alone does not prove mutation support.

## Bound arithmetic and INSERT-from-SELECT

Structured assignment arithmetic is implemented as `arithmetic_operand`,
`arithmetic_expr` and immutable `arithmetic_ptr` nodes in `value_source`.
`sql::add` / `sql::multiply` accept mapped runtime column leaves, encoded values,
or nested arithmetic. The compiler quotes identifiers, binds every value, and
parenthesizes every operation. `expr_ast` and the SELECT/predicate DSL are unchanged.
Literal/raw assignments retain their existing construction. Rebuild consumers
and update exhaustive `value_source` visitors for its new alternative.

Repository arithmetic validates all referenced columns and encoded errors before
leasing. Primary/generated source columns can be read; destination protections
remain in force. Null pointers, invalid operators, depth beyond 64 operations,
and more than 1024 operand/node visits per expression reject. Shared subtree
visits count separately, bounding malformed cycles and exponential DAG expansion.
No raw/function operands or automatic casts are introduced. SQL types, nullable
arithmetic, overflow and coercion remain backend responsibilities. Cross-target
assignment reads retain native SET evaluation order; the
[guide](how-to/bulk-predicate-mutations.md) documents the PostgreSQL/SQLite versus
MySQL difference and portable single-target usage.

The implemented INSERT entry point is `insert_select<T>(sql::select_ast source)`
returning `result<size_t>`. The dedicated `insert_select_ast` carries target
table, destination columns, and SELECT source. Keeping `insert_ast` rows-only
avoids ambiguous simultaneous VALUES/SELECT states. The compiler owns the prefix
and source rendering, retaining one parameter vector and statement.

Its first implementation enforces:

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
- On MySQL, reject matching final source/destination table-name components
  (ASCII case-insensitive), even across different schema prefixes. This is a
  conservative syntactic guard; it does not resolve views, server aliases or
  Unicode normalization, and does not issue catalog queries before insertion.
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

**Next step:** combined bulk predicate DML acceptance review. Checked counts, literal
UPDATE/DELETE, structured assignment arithmetic and INSERT-from-SELECT are implemented; the feature remains partial until the required
remaining APIs and combined acceptance checks close.

## Historical verification for the initial inventory

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

That capture is the historical baseline: it does not test the later repository
APIs or large-count workloads. Its absence assertions intentionally no longer
compile after implementation; do not rewrite the frozen evidence. No ClickHouse
runtime behavior, remote CI, or release result is claimed. The locking/timeout evidence remains a historical snapshot and is
not rewritten by this review.

## Implementation follow-up: predicate UPDATE/DELETE (2026-11-07)

The [repository](../include/asterorm/core/repository.hpp) now validates target
mapping, assignment lists, predicate operator categories/children, and encoded
parameter errors before acquisition, then compiles once and executes once through
the session observer pipeline. Versioned entities and missing/unsupported explicit
capabilities reject. Zero matches succeeds. Existing native SQL/builder APIs keep
their original lower-level behavior.

[No-server tests](../tests/unit/test_bulk_predicate.cpp) cover wide count parsing,
checked signed/32-bit conversions, literal/null/raw values, dialect/parameter order,
codec/lease/database/observer failures, keyless/composite/generated/version mapping,
malformed ASTs, and disabled/unknown/ClickHouse capabilities. Native adapter unit
cases cover wide result storage and PostgreSQL handle ownership/failure/move paths.
SQLite checks direct counts with RETURNING and triggers and prevents SELECT/DDL
from inheriting stale DML counts.

The [shared live fixture](../tests/support/bulk_predicate.hpp) covers three
backends: typed-predicate lowering, flag changes, archive-after-cutoff deletion,
no-change/zero-match behavior, generated/default values, constraint errors,
rollback, commit, and observer exceptions after an autocommit write. PostgreSQL
[timeout cases](../tests/integration/test_statement_timeout.cpp) exercise both
methods under pool and scoped timeouts, SQLSTATE 57014, explicit rollback, physical
connection reuse, and later successful work.

Fresh verification for this slice:

- `just test`: `Ok: 14`, `Fail: 0`, including the compiled public mutation guide.
- `just test-pg`: `Ok: 3`, `Fail: 0`; `just test-sqlite`: `Ok: 2`, `Fail: 0`.
- `meson compile -C buildDir` and `just format-check`: exit 0.
- `just lint`: exit 0 across 50 translation units. New diagnostics were corrected;
  final targeted lint of MySQL execution and the two edited unit files, then the
  final core-unit lint, each exited 0. Remaining warnings are existing/third-party.
- Following those nonfunctional test-initializer fixes, the core bulk-DML test
  selection and PostgreSQL unit tests pass again. ClickHouse header/rejection
  instantiation compiles without a client library or server.
- A fresh build/install with all three relational adapters, tests/examples disabled,
  and a temporary prefix was consumed through both `AsterORM::asterorm` in CMake
  and shared-library `pkg-config --cflags --libs asterorm`: build/run exit 0 for
  UPDATE/DELETE against SQLite. Private package requirements include SQLite
  `>= 3.37.0`. CMake emits an existing SQLite imported-target deprecation warning.
  An extra `--static` experiment hit a missing external zstd search path; static
  transitive linking is not part of this successful shared-consumer result.
- Strict standards validation (`--mode strict-governance --workspace --today
  2026-11-07`): exit 1, retaining 53 existing errors and 12 warnings, with no
  findings on the three edited agent contexts.

These are local commands, not remote CI or release validation. Large native
mutation counts are checked with synthetic parser/result boundaries, not billions
of live rows. Earlier native captures remain historical; the executable API tests
establish that slice's behavior. The later arithmetic follow-up below supersedes
its remaining-work status; INSERT-from-SELECT remains unimplemented.

## Implementation follow-up: structured arithmetic (2026-11-07)

Compiler/no-server tests cover addition and multiplication, both nesting orders,
three dialects, stable parameter ordering, quoted hostile identifiers, owned
values, binding of SQL-looking input, malformed/null nodes, codec errors, depth
and node-budget boundaries, cycles and shared subtrees. Repository tests verify
mapped-operand validation before acquisition and retained primary/generated-target
protections. Existing literal/raw, typed SELECT and predicate paths remain covered.

The shared live fixture applies a bulk price adjustment to two rows while leaving
an unrelated row unchanged, reads a generated source column, propagates nullable
arithmetic, verifies native cross-assignment evaluation, reports NOT NULL errors,
and restores values by caller rollback on PostgreSQL, SQLite and MySQL. PostgreSQL
also checks invalid numeric text (`22P02`) and arithmetic UPDATE under pool/scoped
timeouts (`57014`) with rollback and physical connection reuse. The guide checker
now compiles both literal and arithmetic examples.

Fresh verification:

- `just test`: `Ok: 14`, `Fail: 0`, including both guide snippets, all three live
  relational backends, and existing typed/compiler/migration tests.
- `meson compile -C buildDir` and `just format-check`: exit 0.
- `just lint`: exit 0 over 50 translation units. New test diagnostics were fixed;
  final targeted lint of `test_ast.cpp` and `test_statement_timeout.cpp` exited 0,
  with the remaining AST diagnostic on an unchanged pre-existing upsert fixture.
- After those test-style fixes, `meson test -C buildDir 'core unit tests'
  'pg integration' --print-errorlogs`: `Ok: 2`, `Fail: 0`.
- A fresh temporary-prefix build/install and CMake/shared pkg-config consumers
  compile and execute nested arithmetic (`(1 + 2) * 3 = 9`) followed by predicate
  deletion: exit 0. This checks the new variant alternative across the installed
  header/library boundary; it does not establish other platforms or static linking.
- Strict standards validation (`--mode strict-governance --workspace --today
  2026-11-07`): exit 1 with the existing 53 errors and 12 warnings; no findings
  on the three edited canonical contexts.

INSERT-from-SELECT and combined feature acceptance are next; no full-feature,
remote-CI or release result is claimed. Original inventory captures remain unchanged.

## Implementation follow-up: INSERT-from-SELECT (2026-11-07)

`repository::insert_select<T>(select_ast)` now infers destinations in mapping
order, excluding both generated categories, then uses the same single observed
execution/count path as the other bulk mutations. A separate `insert_select_ast`
keeps VALUES and SELECT sources disjoint. Compiler validation precedes acquisition
for both the repository and direct AST use; the source then lowers through the
existing SELECT compiler, preserving its parameter vector.

The source is one table with explicit matching-width column projections, optional
aliases, portable pagination, and column/parameter predicates. Functions, joins,
GROUP BY/HAVING, locking, wildcards, mixed projection lists, invalid operators/
children and stored codec errors reject locally. Predicates are bounded at depth
64 and 1024 nodes. Server schema/type resolution remains server-owned. Versioned
targets can project their non-generated version column without optimistic checks.
MySQL's conservative ASCII basename guard is documented in the guide; it is not
live-schema object-identity validation. Existing ordinary SELECT/VALUES behavior
and the `insert_ast` layout remain unchanged.

No-server tests cover three-dialect SQL/parameter parity, alias/identifier quoting,
source shape, depth/node boundaries, generated-only targets, version fields,
capability admission and error/observer paths. The shared native fixture copies
nullable/text data with aliases and pagination, verifies generated identity/default/
computed values, zero-row sources, local arity rejection, duplicate errors,
rollback, typed source lowering, commit and observer exceptions after autocommit.
PostgreSQL timeout coverage now includes INSERT-from-SELECT as well as arithmetic
UPDATE and DELETE under pool/scoped limits, explicit rollback, and physical reuse.

Fresh verification:

- `just test`: `Ok: 14`, `Fail: 0`, including all three guide examples, typed compile
  checks, existing APIs, and PostgreSQL/SQLite/MySQL integration cases.
- `meson compile -C buildDir` and `just format-check`: exit 0.
- `just lint`: exit 0 across 50 translation units. New test-style diagnostics
  were corrected; final targeted lint on the edited unit/timeout sources and
  final core-unit lint exited 0. Remaining diagnostics concern unchanged fixtures.
- After those test-only fixes, core/PG integration selection printed `Ok: 2`,
  `Fail: 0`; the final focused INSERT SELECT core selection printed `Ok: 1`,
  `Fail: 0`. Actual disabled ClickHouse header/API instantiation syntax-checks
  without its client library or server.
- Fresh temporary-prefix build/install plus CMake and shared pkg-config consumers:
  compile/run exit 0. The consumer executes arithmetic, copies with INSERT SELECT,
  and verifies the copied value through predicate deletion across the installed
  header/library boundary. Other platforms/static linking are not claimed.
- Strict standards validation (`--mode strict-governance --workspace --today
  2026-11-07`): exit 1 with 53 existing errors and 12 warnings, no findings on the
  three edited canonical contexts.

The next task is combined bulk predicate DML acceptance; this implementation
slice does not claim that review or remote CI/release validation is complete.
Earlier captures remain historical.
