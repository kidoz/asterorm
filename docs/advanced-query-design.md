# CTEs, windows and set operations: inventory and implementation contract

Reviewed on 2026-11-08. **Ordinary CTEs, set expressions and the query-root/execution foundation are
implemented, including constrained recursive CTEs, inline windows/typed helpers,
PG/SQLite FILTER and PostgreSQL DISTINCT ON; combined review remains planned.** The [capture](evidence/advanced-query-inventory/README.md)
compiled the then-current AST probes and executes native SQL on PostgreSQL 16.14, SQLite
3.54.0 and MySQL 8.4.13. Those observations establish feasibility and constraints,
not implementation or acceptance of the proposed API below.

## Inventory before implementation

| Area | Current behavior | Design consequence |
| --- | --- | --- |
| [Runtime AST](../include/asterorm/sql/ast.hpp) | One `select_ast` leaf, mandatory table, string/functional projections, joins, predicates, grouping, ordering, limits and locks; no CTE/query-expression/set node | Add a query-expression root without changing existing callers' `select_ast` return type |
| SELECT expressions | `expr_ast` has column, parameter and mutable shared function nodes. Assignment arithmetic is a separate AST; SELECT has no structured arithmetic or parameter-only select item | Recursive acceptance should traverse real parent/child rows, not require a new arithmetic DSL or a table-less counter SELECT |
| [Compiler](../src/sql/compiler.cpp) | One fresh parameter vector per `compile(select_ast)`; projection arguments, JOIN ON, WHERE and HAVING bind in that order | Nested queries require one shared compilation context. Concatenating separately compiled child SQL would restart `$1` and misbind |
| Aliases and validation | Function aliases live in `func_expr`; `select_item.alias` is ignored when `fn` is present. Predicate/function recursion lacks a query-wide budget/null/cycle guard; encoded parameter errors can survive compilation | Normalize projection aliases, validate structured graphs and reject encoded errors before acquiring a connection |
| SQLite pagination | Compiler emits bare OFFSET without LIMIT; native SQLite rejects it | Shared SELECT rendering needs SQLite's `LIMIT -1` normalization for offset-only queries, while retaining PG/MySQL behavior |
| [Typed DSL](../include/asterorm/sql/typed.hpp) | Single-shot builders move out `select_ast`; mapped member/type checks, one entity tag, explicit/inferred positional projections | Existing typed leaves can be embedded. Do not pretend to infer arbitrary CTE relation schemas or union result types |
| [Typed execution](../include/asterorm/sql/typed_query.hpp) | Compile before `with_connection`, prefer `observed_execute`, hydrate whole entities or positional DTO/tuples | Reuse observation/codec behavior. New query roots need an explicit mapped-result execution entry point |
| Native/repository paths | `native_as` maps named traits from variadic arguments. INSERT SELECT and entity streams deliberately validate narrow single-table `select_ast` shapes | Do not pass a parameter vector through variadic encoding or silently unwrap an advanced query into those APIs |
| Dialects | Compiler defaults to PG; ordinary legacy facades can inherit that fallback. MySQL library baseline is 8.0.19 | Advanced execution needs an explicit dialect; MySQL's later set operators need an explicit version profile |

The [AST probe](evidence/advanced-query-inventory/ast_probe.cpp) records the binding
sequence `10,20,30,40,50`, ignored function-item alias, deferred encoded error and
SQLite OFFSET SQL. Null/cyclic legacy function graphs are inspected in source,
not executed as crash/stack-exhaustion probes. Existing raw SQL/function fragments
are trusted escape hatches and do not count as structured feature support.

## Native findings and planned capability policy

The [native results](evidence/advanced-query-inventory/results.json) verify bound
CTE/outer predicates, repeated CTE references, a four-node recursive tree, ranking,
navigation, explicit/default frames, nullable ordering, duplicate/NULL set semantics,
operator grouping and dialect rejection. All changes use connection-local TEMP data.

| Construct | Native observations | Planned v1 emission |
| --- | --- | --- |
| Read-only ordinary/recursive CTE | All three configured engines succeed | PG/SQLite/MySQL; recursive shape restricted below |
| Ranking/navigation/ROWS windows | All succeed; default `last_value` differs from a full-partition frame | All three, with explicit semantics and bounded structured frames |
| Aggregate FILTER | PG/SQLite succeed; MySQL rejects | PG/SQLite, including supported aggregate windows; no CASE emulation |
| DISTINCT ON | PG succeeds; SQLite/MySQL reject | PG only |
| UNION / UNION ALL | All succeed | All three |
| INTERSECT / EXCEPT | All current engines succeed | PG/SQLite; MySQL requires the modern profile |
| INTERSECT ALL / EXCEPT ALL | PG/MySQL succeed; SQLite rejects | PG, modern-profile MySQL; SQLite rejects without emulation |
| DISTINCT inside window aggregate | All configured engines reject | Reject locally |
| Direct parenthesized compound arms | PG/MySQL succeed; SQLite rejects the tested form | Portable derived-query rendering, preserving the explicit tree |
| ClickHouse/unknown advanced execution | Not executed | Reject before leasing in v1 |

MySQL [set operations](https://dev.mysql.com/doc/refman/8.0/en/set-operations.html)
added INTERSECT/EXCEPT at 8.0.31. `query_compile_options` therefore has an
explicit MySQL profile: baseline `8.0.19` permits CTE/windows/UNION, while a caller
asserting `8.0.31+` enables all set operators. Default remains baseline; invalid
profile values reject. Compiler/executor performs no hidden version query or lease
to discover capability. Integration exercises the modern profile on configured
8.4; no older-server execution is claimed by this inventory.

SQLite compounds [associate left to right](https://www.sqlite.org/lang_select.html#compound_select_statements),
while PG [gives INTERSECT higher precedence](https://www.postgresql.org/docs/16/queries-union.html).
The native mixed expression returns one row on SQLite versus two on PG/MySQL.
The AST's grouping must determine results, independently of those defaults.
Derived-query wrappers pass on all three engines; generated wrapper names and
leftmost output labels must survive normalization.

MySQL [TEMP table limitations](https://dev.mysql.com/doc/refman/8.4/en/temporary-table-problems.html)
matter for tests: the captured repeated CTE reads over one TEMP source fail with
1137, including the tested DISTINCT body. Reuse over literal rows succeeds.
The recursive fixture separates root/edge sources, and set fixtures use separate
left/right TEMP sources. The compiler must not insert DISTINCT or promise CTE
materialization to hide native planning/resource behavior.

PG [window semantics](https://www.postgresql.org/docs/16/functions-window.html) and
SQLite's [window reference](https://www.sqlite.org/windowfunctions.html) distinguish
partition, ordering and frame. Captured first/last/nth values and missing navigation
rows are nullable. Default NULL ordering also differs in the probes; v1 keeps
native ordering and does not promise cross-backend equality for NULL/peer ordering
without a sufficiently explicit user query. No automatic tie-breaker is added.

## Use cases and invariants

1. Filter a relation in a bound CTE, then bind a second predicate in its consumer;
   return explicitly mapped rows through one observed SELECT.
2. Traverse an acyclic parent/child fixture using anchor UNION ALL recursive step;
   the server computes recursion, without a client iteration loop or hidden commits.
3. Rank rows by partition, inspect previous/next values, compute running aggregates,
   and choose full-partition first/last/nth frames deliberately.
4. Combine equally wide rowsets, selecting distinct or multiset semantics and
   controlling final ordering/pagination without relying on backend precedence.
5. Use PG DISTINCT ON or PG/SQLite aggregate FILTER through explicit capability
   checks, preserving native NULL, type, collation and error semantics.

Every operation is one parameterized read statement. Compilation/structural
validation precedes leasing and observation. Rejection sends no SQL/event; successful
dispatch uses one normal SELECT observation and the existing session's transaction,
timeout and testing-scope gates. Native errors retain diagnostics; no retry, hidden
transaction, schema introspection, materialization promise or performance claim.

## Planned AST and public surface

The ordinary CTE/root/execution and set-expression subsets below are implemented;
constrained recursion, windows/typed helpers and extensions are implemented.
See the [CTE guide](how-to/common-table-expressions.md) and
[extension guide](how-to/aggregate-extensions.md) for current spellings.

- Keep `select_ast` as the existing leaf. Add a move-only `query_ast` envelope with
  ordered CTE definitions, one `recursive` flag, and a body variant of `select_ast`
  or `compound_ast`. CTE definitions own name, optional output-column names and
  `unique_ptr<query_ast>` body. Compound nodes own operator/quantifier and left/right
  query children plus their own final order/limit/offset. A simple leaf keeps its
  current tail; there is no second competing envelope tail.
- Query edges have unique ownership, matching existing predicate ownership. Wrapping
  moves a leaf; builders are single-shot. Existing shared function references retain
  their legacy aliasing semantics; no implicit deep-copy/immutability promise.
  Compilation is non-reentrant and callers must not mutate a graph concurrently.
- Add `sql::query(select_ast)` returning `query_builder`, `.with_cte(name, query_ast,
  columns={})`, `.recursive(bool=true)` and `.build()`. Definitions retain insertion
  order. Add `compiler::compile(const query_ast&)` and query compile options without
  removing existing compile overloads.
- Add `sql::query_as<Row>(Session&, const query_ast&, query_compile_options={})` in
  a separate `sql/query.hpp`. `Row` declares `entity_traits<Row>::columns`; table
  metadata is not used to invent CTE mappings. Compile/check before `with_connection`,
  use `observed_execute`, and hydrate explicitly named output columns with existing
  codecs. Reject missing/duplicate result labels rather than choosing an ambiguous
  first column. Empty results return an empty vector; NULL/type failures are errors
  unless the destination is compatible. This is a materializing read path.
- Add free `union_`, `union_all`, `intersect_`, `intersect_all`, `except_`, `except_all`
  taking owned query values and returning a query builder. Chaining groups by the
  construction tree, not SQL precedence. Final compound ordering names unqualified
  output labels only. Branch-local tails are explicitly wrapped and remain local.
- Extend function nodes with a shared-const owned FILTER predicate and inline
  `over_clause`, preserving legacy function-node copyability.
  Add runtime window helpers and typed selectable counterparts in the window
  delivery. Preserve one AST/compiler, with typed code lowering into it.

Ordinary CTE use (declare the mapped row and session as in the guide):

```cpp
auto eligible = sql::select({"id"}).from("events")
    .where(sql::col("id") > sql::val(10)).build();
auto consumer = sql::select({"id"}).from("eligible")
    .where(sql::col("id") < sql::val(100)).build();
auto report = sql::query(std::move(consumer))
    .with_cte("eligible", sql::query(std::move(eligible)).build(), {"id"})
    .build();
auto rows = sql::query_as<eligible_row>(session, report);
// PG: WITH "eligible"("id") AS (... id > $1) SELECT ... id < $2
// parameters: 10, 100; MySQL uses positional ? in the same occurrence order.
```

Existing typed builders can supply moved leaves through `.build()` when their
outputs meet the explicit-label rules below. Existing
`typed::query` remains supported. Arbitrary CTE schema inference and statically
checked compound result unification are deferred; explicit mapped result rows are
the v1 boundary for advanced roots. Typed window results use `int64_t` for ranking,
and conservative optional value types for navigation/frame-value helpers; unsupported
argument/entity combinations must fail with clear compile-time diagnostics.

## Validation, binding and rendering rules

One compilation context owns dialect/options, parameter vector, path/diagnostics,
generated aliases and traversal accounting. It renders CTEs in declaration order,
then the main body; compounds visit left before right. Within a leaf it follows
projection arguments, FILTER, OVER arguments, JOIN ON, WHERE and HAVING in emitted
SQL order. Repeated occurrences append repeated parameters, even for shared nodes
or equal values. No child compilation resets indexes and no SQL-string renumbering
is allowed. Encoded errors and text NULs reject before acquisition; SQL NULL stays
distinct from an encoding failure. Identifiers/frame constants are not data binds.

The new root validator has maximum depth 64 (root depth zero), 4096 visited
query/expression/predicate occurrences and 64 CTE definitions per statement. Shared function DAG occurrences
count separately; column occurrences in projection/group/order lists count too.
Active-path cycles and null/invalid variant/operator nodes reject.
Limits are library syntax budgets, not server recursion/result/parameter guarantees.
No partial SQL/result is returned on failure. Compiler reuse after failure resets
all context state. Existing valid SELECT SQL/parameter ordering stays stable except
explicitly corrected alias/SQLite pagination cases below.

CTE names/output aliases are single ASCII identifiers (`[A-Za-z_][A-Za-z0-9_]*`),
limited to 63 ASCII bytes and quoted for the dialect. Dots, wildcard, empty/NUL
names and case-insensitive duplicates reject conservatively.
CTE reference spelling must match its declaration exactly; reject a case-only
near-match locally instead of allowing dialect-specific relation lookup.
Each select item has exactly one column/function source. Main/CTE outputs have
unique explicit names; plain columns use their unqualified basename and functions
require an explicit alias. Advanced roots reject wildcard/implicit projections. CTE column lists
must match body arity. Forward references, mutual recursion and nested WITH/shadowing
are deferred; WITH belongs at the statement root, not inside operands/CTE bodies,
and the compiler does not hoist definitions. Earlier definitions are visible, with self-reference only in the
recursive form below. Qualified base tables do not resolve as CTE references.
Live schema/type/collation compatibility remains native; no full SQL semantic checker.
Table-less SELECT, VALUES query roots and general SELECT arithmetic remain outside
v1; native literal/counter syntax does not imply those AST extensions are required.

Function aliases belong at projection boundaries. Normalize legacy `func_expr.alias`
and `select_item.alias`: emit once when equal, accept either alone, reject conflicts
or aliases nested inside expressions. Correct the currently ignored function-item
alias. SQLite offset-only rendering uses `LIMIT -1`; MySQL retains its existing
sentinel LIMIT, PG its native OFFSET. These adjacent fixes need regression and
migration notes at implementation, not silent changes in this inventory.

Read-only advanced roots reject locks, DML CTEs, raw projection fragments (except
structured COUNT(*)), and unsupported feature/profile combinations. Existing native
SQL and legacy raw fragments remain trusted escape hatches outside structured
validation; they are not a way to claim missing AST features are implemented.
SELECT-only AST shape is not a purity guarantee for user-chosen server functions
or an SQL sandbox.
Existing INSERT SELECT/stream overloads remain `select_ast`-only: no implicit
conversion may strip a WITH/set/window node or broaden those accepted shapes.

## Feature-specific bounds

**Recursion:** one self-referencing CTE, two plain SELECT arms joined by UNION ALL;
explicit output columns and equal arity. The anchor has no self-reference; the
step has exactly one top-level self table in FROM/INNER JOIN. Step projections and
predicates use columns/parameters only, with existing logical/NULL comparisons;
function calls are deferred there. No step aggregates,
windows, DISTINCT, outer joins, grouping, nested WITH or local order/pagination.
For portable direct SELECT emission, the anchor and UNION connector also reject
local order/pagination; use the consumer's tail. Non-self CROSS joins follow normal
validation, while a self JOIN must be INNER. Step parameter values appear in
predicates because the existing AST has no parameter-only projection item.
Ordinary earlier CTEs remain available. SQL self-reference is a relation name, not
a C++ graph cycle. Users supply terminating data/predicates and server limits;
the API does not promise to detect data cycles, bound recursion time or reorder work.
The native graph fixture is the required live acceptance case.

**Windows:** inline PARTITION BY and ORDER BY column lists, optional bounded ROWS
frame with checked nonnegative integer offsets, CURRENT ROW and unbounded ends.
Validate legal start/end ordering and offset range before emitting numeric literals.
No named windows, RANGE/GROUPS/EXCLUDE, NULL-treatment options, FROM LAST, nested
windows or DISTINCT window aggregates in v1. Ranking helpers require order keys;
frame-value helpers require an explicit frame to avoid surprising defaults.
Navigation offsets/nth positions are checked constants (nth starts at 1), with
optional bound defaults. Output order still requires the final query ORDER BY.

**Set operations:** explicit binary tree and equal nonzero arity, with unique output
labels taken from the left side. Types/coercions/NULL duplicate equivalence and
collations remain native. DISTINCT is the default; ALL preserves native multiplicity.
No SQLite multiset emulation. Derived wrappers preserve nested grouping and local
tails on all supported dialects, without exposing generated names as result labels.
Final ORDER BY/LIMIT/OFFSET applies once to the full compound; deterministic results
need user-specified order. MySQL modern-profile requirements are checked pre-lease.

**Extensions:** PG DISTINCT ON accepts column keys, requires matching leftmost ORDER
BY keys and an explicit user tie policy; no implicit latest-row rule. FILTER accepts
only the known COUNT/SUM/AVG/MIN/MAX aggregate family and a structured predicate;
it is emitted before OVER when
both are present. No built-in ranking/navigation FILTER or aggregate-in-WHERE
convenience that would silently require a derived query. MySQL FILTER rejects.
OVER admits that aggregate family and the listed window helpers, with checked
arity/context; unknown aggregate/window functions are deferred, while ordinary
legacy scalar functions retain their existing native resolution behavior.

## Delivery and acceptance plan

| Delivery | Required evidence before completion |
| --- | --- |
| **Implemented: ordinary CTE foundation** | Owned query root/builder, shared context and structural budgets; ordered nonrecursive CTEs with column aliases; explicit mapped-result execution/observation; corrected alias/pagination validation. Golden PG/SQLite/MySQL SQL and bind ordering, zero-lease rejection, codec/NULL/empty results, one event, caller rollback and testing-scope use; native bound CTE results on all three; existing suites and installed consumers |
| **Implemented: set expressions** | Binary operators/quantifiers, explicit grouping and derived wrappers; arity/labels/profile checks. Duplicate/NULL cases, mixed precedence and branch/final tails; baseline/modern MySQL rejection/admission; native bag/set results |
| **Implemented: recursive CTE** | Anchor/step validation, SQL self-reference separated from AST cycles; live parent/child traversal and bound predicates; malformed/forward/mutual/nonterminating cases handled within the stated syntactic/native boundaries |
| **Implemented: windows and typed helpers** | OVER/FILTER ownership, types and frame validation; ranking/peer/NULL/navigation/running/full-partition cases; static argument diagnostics and identical runtime/typed lowering |
| **Implemented: extensions** | PG DISTINCT ON and PG/SQLite FILTER, including window aggregates; explicit tie policy, backend rejection, bounded predicates and no silent emulation; typed/native parity and installed guide consumers |
| **Next: combined acceptance review** | Cross-feature CTE/set/recursion/window/extension composition, current public guides and fresh consumers; source-bound regression/build/format/lint/standards evidence and explicit layout/version notes across the complete subset |

The first delivery is deliberately ordinary CTEs plus the query-root/execution
foundation. It does not expose unimplemented operations as successful no-ops:
windows admit the inline subset and recursive roots admit only the constrained
form below. FILTER and DISTINCT ON admit the extension subset below. Set expressions are now available. The pre-implementation inventory capture remains historical; do not rerun its
old gap assertions as current feature tests. The ordinary CTE implementation changes
`compiler` and `compiled_query` layout: rebuild the library and consumers together.


## Ordinary CTE implementation follow-up (2026-11-08)

`sql/query_ast.hpp`, `compiler::compile(query_ast)` and `sql/query.hpp` now provide
owned envelopes, ordered nonrecursive definitions and explicit named materialization.
All SELECT leaves render through one shared parameter vector. Query validation
counts occurrences across every definition/main leaf, checks active-path function
cycles and validates before dispatch. At this delivery, compound bodies and
recursive envelopes rejected locally; compound bodies are enabled by the follow-up below. Narrow INSERT SELECT/stream interfaces remain unchanged.

The [guide](how-to/common-table-expressions.md) states the delivered contract.
Advanced session facades require a compatible observer helper; there is no direct
execution fallback. Mapped names are checked against compiled labels before leasing;
native width/ordered labels are checked even for zero rows. Extra selected columns
are allowed, codec/native diagnostics and observer exceptions retain their semantics.
The SELECT alias, encoded-parameter and SQLite pagination corrections described
above are implemented, including legacy SELECT callers.

The [delivery evidence](evidence/ordinary-cte/README.md) records local checks.
At foundation delivery the next task was explicit set-expression trees and
portable derived wrappers, with arity/label checks and MySQL profile admission.
That follow-up and the recursive/window/extension deliveries are now implemented
below. This foundation does not mark
the entire advanced-query plan accepted.


## Set-expression implementation follow-up (2026-11-08)

The six owned set factories and query-builder tails are implemented. Validation
walks every operand using the same occurrence/depth/CTE visibility context, checks
non-null branches, operator/quantifier values, equal nonzero arity and exact left
output ordering labels, and enforces MySQL/SQLite capabilities before dispatch.
Projection labels propagate from the left. Server types, coercions and collation
remain native, and no casts or multiset emulation are inserted.

Each operand renders as a derived SELECT with an alias scoped outside its child.
This preserves the construction tree and local tails even on SQLite. CTE bodies
may contain compounds, but WITH remains statement-level. All CTE and main operands
share parameters in emitted left-to-right order; generated aliases also use a
statement-local counter. Native fixtures use separate TEMP sources and verify
bag/NULL results, opposing groupings, local/final tails and empty-result metadata.

The [guide](how-to/set-operations.md) and
[delivery evidence](evidence/set-operations/README.md) cover mapped execution,
backend profiles and local verification. The AST had reserved compound storage;
this delivery activates it and adds inline builder methods, without another data
layout change. The CTE foundation's rebuild and migration notes still apply.
Older inventory/CTE receipts remain historical.

At set delivery the next task was the constrained recursive form above: a two-arm
UNION ALL CTE, checked anchor/step references and a real parent/child traversal.
That follow-up, windows, typed helpers and extensions are implemented below.
Combined advanced-query acceptance remains next.


## Recursive CTE implementation follow-up (2026-11-08)

The statement-root recursive flag now admits exactly one self-referencing CTE
with explicit output columns and direct anchor/step UNION ALL. A bounded shape
check recognizes only a plain SELECT step, and the normal graph validation shares
budgets, bindings and CTE visibility across both arms. Self visibility is limited
to the checked step; nested flags, extra self relations, forward/mutual references
and step functions/grouping/outer joins reject locally. Ordinary definitions may
precede/follow the recursive definition, and a compound consumer stays supported.

Recursive arms bypass ordinary derived wrappers so self references remain legal
on all three backends. The connector and both leaves reject local tails, including
the anchor: this is the explicit portability refinement of the original step-only
tail restriction. Consumer tails remain available. No new AST fields or native
adapter paths are added; the existing named observed materialization path is used.

The [guide](how-to/recursive-common-table-expressions.md) and
[delivery evidence](evidence/recursive-cte/README.md) record golden SQL, graph/
shape/no-lease checks, real nullable parent/child traversal, UNION ALL duplicates,
caller rollback and supported scopes. A PG data-cycle test uses an explicit server
timeout and rolls back inside its callback before cleanup, preserving the existing
connection/SQLSTATE behavior. No unbounded cycle executes on SQLite/MySQL. Syntax
admission of a potentially nonterminating echo step is tested without executing it:
termination, visited sets and native resource limits are not compiler guarantees.

At recursive delivery the next task was inline windows and typed helpers with
checked frames/result nullability. That follow-up and FILTER/DISTINCT ON are
implemented below; combined review remains next. The broader advanced-query
feature remains in progress; earlier captures stay historical.


## Window implementation follow-up (2026-11-08)

Runtime OVER/ROWS helpers and typed counterparts now lower into the same function
nodes and compiler. Window specs own column/order/frame data, with checked signed
constants, legal frame categories/offset ordering, function arity and projection
context. Ranking requires order keys; frame-value functions also require explicit
frames. DISTINCT/unknown/nested windows and predicate windows reject. Recursive
steps retain their function ban. Shared-const FILTER ownership was reserved at
window delivery and is activated by the extension follow-up below.

The typed selectable concept now accepts const instance lowering; static methods
still work. Projection construction retains values. Entity/order/frame/argument
and DTO diagnostics reject incompatible typed calls. Navigation/frame-value and
window aggregate values are conservatively optional, except COUNT/ranking int64.
Typed windows require explicit supported dialect metadata and compatible observer
execution; ordinary typed compatibility facades retain their prior fallback.

Window metadata participates in global budgets/cycle checks. An iterative
presence scan limits the new bounded legacy SELECT preflight to statements with
structured window data, preserving the existing stream boundary and plain SELECT
behavior. Primitive numeric hints in param_expr emit numeric casts throughout a
window statement on SQLite/MySQL: SQLite computed CTE columns lack base-column
affinity and MySQL text-bound navigation defaults otherwise widen to decimal text.
PostgreSQL placeholders and non-window SQL stay unchanged. Payloads remain encoded
text, and adapters are unchanged. Compiler cast scope resets on return/error.

The [guide](how-to/window-functions.md) and
[evidence](evidence/window-functions/README.md) cover native runtime/typed SQL and
result equality, peer/default versus ROWS frames, nullable navigation, empty/full
frames, derived filtering, scopes and compile diagnostics. func_expr, param_expr
and compiler layouts changed; rebuild library/consumers. At window delivery the
next task was PG DISTINCT ON and PG/SQLite FILTER, implemented below. Combined
advanced-query acceptance remains next. Earlier evidence stays historical.

## Aggregate extension implementation follow-up (2026-11-08)

PG/SQLite FILTER now decorates COUNT/SUM/AVG/MIN/MAX, including supported aggregate
windows. The function owns a shared-const predicate; decoration copies the function.
Arguments bind before FILTER, then OVER. Projections and HAVING admit filtered
aggregates; WHERE/JOIN do not. FILTER predicates admit ordinary scalar expressions
but reject aggregate/window/nested FILTER nodes. Global occurrence/depth budgets
and active-path cycle validation include the predicate. SQLite filtered statements
use the existing numeric-cast affinity policy. Scalar DISTINCT aggregate arguments
are allowed; DISTINCT windows remain rejected. COUNT is zero on empty input; the
other filtered results are nullable, including typed DTOs. Typed FILTER is applied
before OVER through `typed_extensions.hpp` and shares runtime lowering.

PostgreSQL DISTINCT ON adds a SELECT-leaf clause with nonempty unique column keys,
exact positional matching against the leftmost ORDER BY names, and a mandatory
`distinct_on_ties` policy. `require_tiebreaker` requires an extra ordering column;
the caller must ensure the full order is unique for deterministic selection.
`allow_peers` explicitly permits arbitrary remaining peer selection. Qualified
spellings are not normalized, no timestamp/ID sort is inserted and no schema
query proves uniqueness. Native NULL/direction/collation rules apply. Locking,
unsupported backends and malformed policies/order reject before dispatch.

Both features retain one observed execution and existing caller transactions,
testing scopes, codecs and errors. INSERT SELECT, stream and recursive-step
boundaries remain narrow. `select_ast` adds a field and containing query layouts
change; rebuild library and consumers together. The
[guide](how-to/aggregate-extensions.md) and
[evidence](evidence/aggregate-extensions/README.md) cover SQL/bind golden tests,
native runtime/typed equality, empty/NULL/HAVING/filtered-window results, PG winner
selection and combined DISTINCT ON/window behavior, zero-lease rejection, graph
limits, compile controls and installed CMake/shared pkg-config consumers.

The next task is a combined acceptance review across CTEs, sets, constrained
recursion, windows and these extensions, with fresh composition cases and a
source-bound aggregate evidence record. Individual delivery checks do not close
that review or establish remote CI/release, performance or older-server coverage.
