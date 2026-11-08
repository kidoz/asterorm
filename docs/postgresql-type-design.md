# PostgreSQL type coverage: inventory and v1 contract

Reviewed on 2026-11-09. **Inventory and v1 contract complete; implementation remains
planned.** The [source-bound capture](evidence/postgres-type-inventory/README.md)
records current APIs and PostgreSQL 16.14 behavior. This document defines the next
work after [production-pool acceptance](production-pool-acceptance.md); it does not
claim that the proposed types/helpers exist or close type-expansion acceptance.

## Current implementation and gaps

| Area | Observed implementation | Required consequence |
| --- | --- | --- |
| Values | types.hpp supplies jsonb, bytea, uuid, date, time_of_day, numeric and enum_traits. Codecs also handle scalars, optional, vectors and system_clock timestamps. | Introduce new public value models with their implementation, preserving existing valid inputs and codec customization priority. |
| Extension point | codec<T> is detected before built-in non-NULL encoding/decoding. Positive compilation instantiates existing values and a custom codec. | Keep custom codecs supported; do not auto-serialize every mapped entity as a composite. |
| Range/interval/network/MAC/hstore | Proposed first-party types/alias are absent. The exact map<string,optional<string>> hstore shape reaches unsupported encoding. | Add explicit supported models and strict text codecs, not string aliases pretending to validate structure. |
| Arrays | Nullable string elements preserve SQL NULL versus literal NULL. Nested vectors encode as a native multidimensional array, but cannot decode it. Non-one lower bounds reject. A malformed flat decode leaves partial output. vector<jsonb> emits invalid array text. | Explicitly exclude multidimensional/dimensioned arrays in v1, harden flat parsing and generalize element quoting before composing new value codecs. |
| Numeric/timestamp boundaries | Numeric decoding rejects native NaN; invalid numeric encoding is passed through without a local error. A malformed timestamp is normalized, and timezone offset seconds are ignored. | Fix these prerequisites before using them as range endpoints. Preserve valid wire precision and reject malformed/trailing/out-of-range input. |
| JSONB | jsonb wraps text without validating JSON. Runtime expression nodes cover columns, parameters and functions; no checked JSON operator family exists. A generic jsonb_path_exists function can compile for SQLite. | Add owned PostgreSQL-specific nodes and typed lowering with pre-lease dialect/context validation. Generic functions/raw SQL remain caller escape hatches. |
| Composites | Hydration maps each selected column to one member and invokes decode. Nested entity_traits do not automatically provide a field codec. | Use explicit composite wrappers, ordered recursive traits and a record grammar; retain ordinary one-column field hydration. |
| Parameter typing | query_parameter carries text/boolean metadata and encoding errors; PG uses text parameters and context inference. No OID registry or structured PG cast node exists. | Keep vendor OIDs out of shared headers. Add checked AST casts for standalone/ambiguous expressions; raw SQL callers supply casts where native inference needs them. |

[Results](evidence/postgres-type-inventory/results.json) contain 185 native/codec
observations and 11 compile controls: one successful current-API control and ten
expected missing/unsupported API compilations. These are historical gap probes,
not failing production tests or acceptance of the future surface.

## Native findings that shape the contract

- Native int4range canonicalizes [1,3] to [1,4), and an empty discrete interval
  becomes empty. SQL NULL, empty and unbounded are different. Explicit timestamp
  infinity is a value bound, not an absent bound. Numeric NaN is a legal range
  endpoint in the captured server. Do not invent a client total ordering or
  normalize native ranges with machine arithmetic.
- The same mixed-sign interval has four different IntervalStyle outputs. It keeps
  14 months, minus three days and a time component; flattening to chrono duration
  loses calendar meaning. Native one-month and 30-day comparison can be equal even
  though adding them to 2024-01-31 produces different dates.
- inet preserves host bits in 192.0.2.9/24; cidr rejects that input with 22P02.
  Their canonical text can otherwise look identical. Use separate C++ intent for
  host/prefix and strict network values, rather than guessing the SQL type from
  output text. MAC output is canonical six-octet text.
- Composite NULL is absent at the result API, while a non-NULL all-null record is
  text (,,). Empty text and NULL fields differ. Nested records add a separate
  escaping layer. hstore likewise distinguishes NULL, literal NULL and empty text.
- JSON extraction distinguishes SQL NULL from jsonb null. jsonb_path_query returns
  multiple rows; jsonb_path_exists is scalar. Parameterized paths/variables work,
  and invalid paths retain native SQLSTATE. Scalar and set-returning expressions
  need distinct admission rules.

The native rules above are also documented by PostgreSQL's
[range](https://www.postgresql.org/docs/16/rangetypes.html),
[date/time](https://www.postgresql.org/docs/16/datatype-datetime.html),
[network](https://www.postgresql.org/docs/16/datatype-net-types.html),
[hstore](https://www.postgresql.org/docs/16/hstore.html),
[composite](https://www.postgresql.org/docs/16/rowtypes.html),
[array](https://www.postgresql.org/docs/16/arrays.html) and
[JSON function](https://www.postgresql.org/docs/16/functions-json.html) references.
The representation/API decisions below are this project's contract choices.

## Value and codec contract

Names below are proposed v1 spellings, not available APIs. Keep models in the
existing public core types/codecs surface, with private parsing helpers as needed.
Shared headers must not require libpq or platform networking headers. Transport,
native OIDs and protocol state remain in adapters. C++23 and text-format binding
remain the baseline; no binary protocol or schema introspection is added.

### Common rules

New decoders parse into temporaries and assign only after complete success.
parse_failed leaves output unchanged. Local encoding failure is an encoding error,
never a valid SQL NULL; optional<T> continues to represent NULL. Embedded NUL,
malformed quoting/escaping, trailing garbage, invalid arity/domain, overflow and
unsupported nesting reject explicitly. Allocation/custom-codec exceptions may
propagate under existing library rules. Parsing must be linear in input size and
bounded in recursive structure; do not split nested text naively on commas.

Keep codec<T> priority over built-in dispatch. Add specific grammar entry points
for arrays, ranges, records and hstore: they have different escaping/null rules.
Do not reuse SQL-literal escaping for wire values. Preserve error kind and native
SQLSTATE/diagnostic fields when validation belongs to the server. No retries,
observer events for rejected pre-lease queries, hidden transactions or session
setting changes are introduced.

Finite dates require ISO calendar text and a checked AD year in the representable
existing date model (1..32767); BC/year-zero/out-of-model dates reject. Finite
system_clock timestamps require checked representability, a valid calendar/time,
full consumption and numeric offsets including seconds (or Z). Do not let timegm
normalize invalid input or duration conversion overflow. Non-ISO DateStyle is an
explicit unsupported wire precondition; the library does not silently SET it.
Existing standalone timestamp microsecond encoding policy stays documented;
range timestamp endpoints must already be microsecond-aligned to avoid moving a
boundary through implicit truncation. Interval output styles are handled below.

Numeric preserves exact decimal text and adds canonical NaN, Infinity and
-Infinity support with strict validation. It never converts through double.
Invalid numeric text becomes a local encoding error when wrapped as numeric;
raw SQL/text bindings retain normal native validation. This intentionally tightens
previously malformed-input behavior; valid existing codecs and enum/custom hooks
must retain regression coverage.

### Ranges

Provide range<T> for int32_t/int64_t/numeric/date/system_clock::time_point, mapped
to int4range/int8range/numrange/daterange/tstzrange. Do not infer tsrange or custom
range names from the same C++ type. Multiranges remain outside v1.

A range has an explicit empty flag and optional lower/upper bounds. A missing
bound means unbounded. Each present range_bound<T> has an inclusive flag and a
closed value/negative-infinity/positive-infinity kind; value storage is present
only for the value kind. Default construction is empty, not unbounded. Empty with
bounds, missing value payloads or payloads on infinity kinds are encoding errors.
Infinity kinds are valid only for numeric/date/timestamp families. Numeric NaN is
a value endpoint; numeric infinity endpoints use the explicit infinity kind so
the range model has one canonical representation. SQL NULL is optional<range<T>>.

Parse native canonical text, quotes and escapes without conflating explicit
infinity with missing bounds. Validate subtype representability and structure,
then let PostgreSQL determine ordering, discrete canonicalization and empty
results. Do not increment maximum integer/date endpoints locally. C++ structural
equality is not a promise of native set equality; compare native-normalized results
and bound flags in integration tests. Native finite endpoints outside the C++
model reject rather than becoming unbounded or wrapping.

### Interval, network and MAC values

interval retains signed int32 months, signed int32 days and signed int64
microseconds. Component equality is structural, not native interval comparison.
Encode an unambiguous unit-bearing text without flattening months or days.
Decode all four PostgreSQL 16 IntervalStyle outputs, including mixed signs,
years-to-months, fractional seconds and verbose ago. Check accumulation and
INT_MIN magnitude handling; never use floating point. Do not change IntervalStyle
on checkout, including behind the transaction-pool profile.

network represents inet: address family, a 16-byte address buffer and prefix length
with the IPv4 portion defined consistently. Add a distinct cidr wrapper over that
value for strict host-bit validation. This resolves the original single-network
proposal's ambiguity: text alone cannot identify which SQL domain was selected.
Use explicit type context/checked casts, not native catalog lookups. Preserve IPv6
and mapped-address family; validate prefix widths. Factories accept numeric
canonical IPv4/IPv6 forms, not DNS names or zone identifiers. Encoding cidr with
nonzero host bits fails; it does not silently mask an inet host. macaddr stores six
bytes and uses canonical colon-separated text. macaddr8 is deferred.

### hstore and composites

Provide hstore as the exact unordered_map<string,optional<string>> alias proposed
by the plan, with specific codec dispatch. Preserve user codec customization.
Encode deterministic key order for reproducible output; parse NULL versus quoted
NULL and empty strings, escapes and delimiters. NULL keys are impossible in the
model. Reject duplicate keys in direct decoder input rather than silently choosing
one; native canonical output is already unique. A NULL hstore is optional<hstore>.
The application/migration owns extension installation and type/schema visibility;
the library never installs hstore. Bound target-column inference works normally;
standalone casts use a supplied checked type reference if needed. Missing native
extension/type errors retain diagnostics.

Provide composite<T> as an explicit owning wrapper using entity_traits<T>::columns
in declared attribute order. Members may contain composite<U> or optional wrapped
values, supporting one and two nested composite levels; deeper structures/cycles
reject with a useful diagnostic. No automatic codec is inferred merely because an
ordinary entity has traits. Keep custom codec<T> behavior intact. A mapped entity
field containing composite<T> occupies one database column, not flattened columns.

The record codec checks exact arity, per-field optionality, empty/NULL text,
escaping and nested grammar. optional<composite<T>> distinguishes SQL NULL from a
non-NULL record whose attributes are NULL. Use explicit qualified PG type metadata
for ambiguous standalone values, with identifier quoting and validation. Do not
look up OIDs/schema or discover layouts during encode/hydrate. Schema/type drift
must fail clearly. Integration must exercise both one-level and two-level mappings
through native reads, repository writes/refresh, typed projections and streaming
where the existing stream source subset supports them.

### One-dimensional arrays

Multidimensional arrays remain **explicitly unsupported** in v1, satisfying the
plan's allowed unsupported decision. Nested vector encoding/decoding must reject
with a deliberate diagnostic; do not retain the current one-way accidental support
or silently flatten. Flat vector decoding rejects dimension prefixes/non-one lower
bounds and nested braces, with output unchanged on error. Empty arrays and nullable
scalar elements remain supported.

Generalize flat element quoting for the new scalar/composite/hstore/range codecs,
including commas, braces, whitespace, quotes, backslashes, empty text and literal
NULL. Preserve existing valid numeric/string array behavior and NULL identity.
An array of a supported composite is not a second array dimension. General
rank/lower-bound metadata, rectangular-shape inference and domain/custom-delimiter
arrays require a separate design.

## Structured JSONB/query contract

Extend the existing expr_ast/compiler and typed lowering; do not build a parallel
query executor. Proposed runtime and typed names are json_get/json_get_text,
json_path_get/json_path_get_text, json_contains/json_contained_by,
json_has_key/json_has_any_keys/json_has_all_keys, jsonb_path_exists and
jsonb_path_query. Closed operator/node enums select SQL tokens; user text never
supplies an operator/function name or raw fragment through these helpers.

- Key extraction uses bound text or integer selectors, with explicit casts to
  resolve operator overloads; text key 1 and array index 1 stay distinct. Path
  extraction binds an explicit vector of text segments, not a dotted SQL fragment.
- JSON-producing extraction returns optional<jsonb>; text extraction returns
  optional<string>. JSON null stays jsonb text null, while text extraction may
  return SQL NULL. Predicate helpers preserve native three-valued logic; they do
  not coalesce SQL NULL to false.
- Containment/key predicates keep the indexed document expression intact. Bind
  JSON values, keys and key arrays; do not inline them or rewrite native operators
  into generic functions. Runtime/typed SQL and bind order must agree.
- jsonb_path_exists binds the jsonpath, JSON variables and silent flag with native
  types/defaults. Invalid path/JSON syntax is validated by PostgreSQL and retains
  SQLSTATE, not a fabricated local SQL error.
- jsonb_path_query is explicitly set-returning: initial structured support permits
  it only as the sole SELECT-list projection over an ordinary source/WHERE,
  producing zero-to-many JSON rows per source row. It is not an entity projection,
  scalar operand, predicate, aggregate/window argument or INSERT/UPDATE/RETURNING
  expression. Reject DISTINCT/group/window/locking compositions in this first
  subset. Typed query execution returns projected JSON tuples/DTOs with checked
  labels. General lateral/FROM SRFs and stream integration for this node are
  follow-up work; the supported SELECT projection is still required for v1.

Add checked PG casts for ambiguous structured parameters using a closed built-in
family or validated qualified composite/hstore type reference. Do not store raw
cast SQL or numeric vendor OIDs in query_parameter; target columns can infer
ordinary bound parameter types. Raw native SQL callers supply explicit casts.

New owned nodes participate in graph/cycle/depth/node-budget validation and the
existing global bind sequence, including containing CTE/set/query roots. Typed
carriers preserve entity affinity, nullability and output type; invalid operand
families or contexts fail before SQL. PostgreSQL-only nodes reject on SQLite,
MySQL, ClickHouse and unknown advanced executors **before lease/observation**.
Existing generic function/raw escape hatches keep their caller-owned semantics.
New text codecs alone do not advertise native PG types on other databases; users
may explicitly store their serialized text, without a native-type/emulation claim.

## Index and acceptance policy

The original GIN-per-type criterion is revised to an index appropriate for each
type/operator family. The captured pg_opclass catalog and six local plan controls
support the distinction; this is not a performance comparison. See PostgreSQL's
[index families](https://www.postgresql.org/docs/16/indexes-types.html),
[GiST](https://www.postgresql.org/docs/16/gist-builtin-opclasses.html),
[SP-GiST](https://www.postgresql.org/docs/16/spgist-builtin-opclasses.html) and
[GIN classes](https://www.postgresql.org/docs/16/gin-builtin-opclasses.html).

| Family | Required native acceptance example |
| --- | --- |
| Ranges | Bound overlap/containment using a compatible GiST or SP-GiST range index; canonical/empty/unbounded/infinity/overflow cases for all five families |
| inet/cidr | Correct host/prefix values and rejection, plus a suitable network index/operator; GiST inet_ops is explicit in the current probe |
| Interval/MAC | Bound equality/order with a compatible B-tree index; preserve component/byte values and native comparison semantics |
| hstore | Bound containment/key work with GIN or GiST and nullable/escaped text round-trips |
| JSONB | Containment/existence with an appropriate GIN class; path result/cardinality tests separately. Do not claim jsonb_path_exists itself automatically uses GIN. |
| Composites/arrays | Correct nested/NULL/arity behavior first; use compatible native B-tree/composite or array indexing only where defined, not a synthetic universal GIN promise |

Index eligibility controls may disable sequential scan locally to inspect a
compatible plan; they must also check query results. No chosen-plan, speedup or
production index recommendation follows. hstore fixtures must either use an
existing installation or create it only inside an owned rollback transaction;
confirm extension/schema state is restored. Never drop/recreate a caller type.

## Delivery and verification order

| Delivery | Exit requirements |
| --- | --- |
| **Next: value/text-codec foundations** | Interval/network/cidr/MAC models and strict codecs; numeric/timestamp prerequisites; reusable bounded grammar/error handling and explicit nested-array rejection. No unsupported type is advertised as implemented. No-server boundary/NULL/overflow/unchanged-output checks plus native scalar/style round-trips and both-backend regression. |
| Ranges and flat array composition | All five range families, tagged endpoints and new scalar array quoting; native canonicalization/infinity/NaN/representability cases and malformed/partial-output prevention. |
| hstore and composite mappings | Exact map alias, explicit wrapper/recursive traits, extension-safe fixtures, one/two levels and NULL/empty/escape/schema-drift controls; writes, reads and supported streams. |
| JSONB runtime/typed nodes | Owned nodes/casts, all listed operators and both path functions with the SRF restrictions above; compile-time affinity/type controls, nested graph budgets, binds, labels and pre-lease rejection on other backends. |
| Combined type acceptance | Fresh full regression, SQLite/MySQL parity/rejections, PG-native indexed query results, sanitizers where applicable, formatting/lint/standards comparison, installed CMake/shared pkg-config consumers, public guides/migration notes and source-bound review. |

Initial native verification targets PostgreSQL 16; older/newer server support is
not inferred merely because some syntax exists there. Keep supported existing
features working on PG/SQLite/MySQL. No runtime capability-discovery query, hidden
extension setup, session GUC mutation, automatic schema generation or binary
transport is part of this contract. Public AST/value/parameter/header changes must
state consumer rebuild requirements when introduced. This inventory adds no
production API or ABI change.
