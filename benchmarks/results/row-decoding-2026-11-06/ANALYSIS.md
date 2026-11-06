# Projection row-decoding evidence

No material typed decoding regression was detected in these workloads. All 54
backend/shape/result-size/phase cases had identical allocation calls and requested
bytes between typed and repository string projections. Median paired timing
ratios ranged from **0.835 to 1.007** typed/string, with no case exceeding the
predeclared investigation screen.

This is evidence for the measured tuple-decoding boundary on this machine. It
does not establish full-query, database/server, network or portable runtime
equivalence, and does not measure arbitrary custom DTO constructors.

## What was measured

The benchmark invokes the actual production helpers:

- Typed projection: `sql::typed::detail::hydrate_dto`, instantiated with the
  selected items and the inferred tuple result type.
- String projection: `detail::decode_projection`, used by repository string
  tuple projections.

Each pair consumes the same immutable backend result object, produces the same
tuple type, and uses the shared codecs. SQL construction/execution, result
acquisition, fixture construction and correctness validation are outside the
timed region. This compares decoder helpers, not entire public query methods.

`row_decode` includes tuple decoding, observation and destruction. `result_vector`
also includes vector reservation, row collection, observation and destruction,
using the common collection pattern of both projection paths.

Native fixtures are populated in memory: PostgreSQL `PGresult` wrapped in
`pg::result`, and the actual SQLite/MySQL materialized result classes. They use
synthetic text fields, including backend boolean spellings, rather than captured
database queries. The exact decimal fixture exercises text preservation, not
database arithmetic/aggregate precision. See the
[method and reproduction commands](../../ROW_DECODING.md).

## Coverage and measurement protocol

- Shapes: integer/string/boolean/double; nullable integer/string/double plus a
  required boolean; aggregate projected types (counts, sum, average, numeric).
- Sizes: 1, 256 and 16,384 rows. NULL differs from an empty string. The one-row
  nullable fixture has all optional fields NULL; larger fixtures mix NULL,
  present scalars, empty strings and long strings.
- Correctness: both decoders must match the fixture value oracle for every row
  and the expected result size. Empty results and NULL/invalid required-field
  errors are verified outside timing. Error kinds and messages must match.
- Apple M4 Max, Apple Clang 21.0.0, C++23, `-O3`, static libraries, LTO disabled;
  runtime libpq version is retained in `verification.txt`.
- Three processes, 21 paired alternating-order samples per case, and 8,192 target
  rows per batch (at least one complete result). Allocation counting uses a
  separate executable, with 256 target rows and at least one complete result.
- The screen was declared before measuring: a paired median ratio above 1.10 in
  every process, or extra allocation calls/requested bytes. No timing assertions
  were added to CI. Dispersion is retained as IQR, not a confidence interval.

## Observations

Ranges below span all three shapes and all three result sizes for each phase.

| Backend | Row decoding: median paired ratio range | Result-vector decoding: ratio range | Allocation calls/bytes |
|---|---:|---:|---|
| PostgreSQL | 0.932–1.001 | 0.930–1.000 | Identical in every case |
| SQLite | 0.835–0.981 | 0.849–1.007 | Identical in every case |
| MySQL | 0.843–0.976 | 0.857–0.989 | Identical in every case |

At 16,384 rows, the row-only scalar workload makes 2 C++ allocation calls /
112 requested bytes per row in both paths. The nullable workload averages 0.5
calls / 28 bytes, and the aggregate workload 2 calls / 80 bytes. Vector collection
adds the same vector storage and amortized allocation to both paths. The full
tables, including smaller-result behavior, are in the [generated report](report.md).

The explicit temporary field tuple in the typed helper does not introduce an
extra heap allocation in these cases. The observed timing differences are not a
universal typed speedup claim: they depend on generated code, row shape, backend
getters, cache behavior and compiler/library versions. Error-path speed and
concurrent decoding were not measured.

## Retained evidence

[Raw timing samples](timings.csv), [allocation counts](allocations.csv),
[machine/build/source metadata](metadata.json), [verification output](verification.txt)
and the machine-readable [summary](summary.json) accompany this interpretation.
There are 6,804 positive timing samples and 108 allocation records, summarized
into 54 comparisons.

Together with the earlier SQL/parameter parity and construction/compiler results,
these measurements support the runtime-cost requirement for the tested client-side
workloads. The next planned deliverable is the incremental migration guide,
followed by a final acceptance review. These results do not mark the whole feature
complete by themselves.
