# M8 DSL baseline interpretation

The benchmark is delivered; the broad M8 unchanged-runtime-cost criterion is
**not established**. There is measurable construction overhead and extra
allocations in the current typed builders. This measurement does not change the
production implementation or narrow the original acceptance contract.

The [generated report](report.md), [raw timing samples](timings.csv),
[allocation measurements](allocations.csv), [AST layouts](ast_layouts.csv), and
[machine/build/source metadata](metadata.json) preserve the baseline. Reproduce it
with the [benchmark instructions](../../README.md), using a new output directory.

## What was observed

Apple M4 Max, Apple Clang 21.0.0, C++23, `-O3`, static core, LTO disabled;
3 processes, 21 paired samples per process, 10,000 operations per timing sample.
Timing uses the standard allocator. Separate allocation measurements count
successful C++ `new` calls and requested bytes.

- Simple SELECT construction: median paired typed/string ratios **1.414** for
  the default compiler dialect and **1.445** for MySQL. Medians were about
  90 ns typed versus 62–64 ns string. Every process exceeded the predeclared 1.10
  investigation threshold. Typed construction made **3 allocations / 168 bytes**
  versus **1 allocation / 96 bytes** for string construction.
- Nullable projection construction: paired ratios **1.099–1.111**; the MySQL
  result did not exceed 1.10 in every process, so the timing signal is
  inconclusive by the predeclared rule. Typed construction made **10 allocations /
  2,984 bytes** versus **9 / 2,536 bytes**.
- Aggregate construction: paired ratios **1.036–1.044**. Typed construction made
  **10 allocations / 1,608 bytes** versus **8 / 1,368 bytes**.
- Construction plus compilation: paired median ratios ranged **1.001–1.098**.
  Compilation dominated these small workloads and reduced the relative effect
  of construction overhead. Extra construction allocations remained.
- Compilation of prebuilt ASTs: paired ratios **0.995–1.029**, with identical
  allocation counts and requested bytes for each matched workload/dialect.

These are per-query client-side costs on this machine. The `postgres_sqlite`
label is a compiler dialect: PostgreSQL/SQLite I/O, MySQL execution, row decoding,
and network effects were not measured. A within-screen timing result is not a
statistical equivalence proof or a portable performance guarantee. IQRs in the
report show sample dispersion rather than confidence intervals.

Construction is independent of dialect. Its two dialect-labelled result sets
repeat the same builder work and should not be interpreted as a backend-specific
construction difference.

## Investigation of the allocation difference

The recorded layouts show `sizeof(std::string) == 24` and
`sizeof(select_item) == 80` on this toolchain.

1. `make_entity_select_ast` appends four columns to an initially empty vector,
   without reserving the known size. The observed allocation sum is
   `(1 + 2 + 4) × 24 = 168` bytes. The string builder initializes a four-element
   vector once: `4 × 24 = 96` bytes. Both finish with capacity four, so final
   capacity alone would conceal the two intermediate allocations.
2. `make_projection_select_ast` similarly appends four aggregate items without
   reserving. The allocation difference is exactly
   `((1 + 2 + 4) - 4) × 80 = 240` bytes and two calls. Both public paths use
   `select_item` for this workload, isolating the growth overhead.
3. A nullable plain-column projection additionally uses different AST storage:
   the typed path uses three `select_item` elements (final capacity four), while
   the string baseline uses three legacy string columns. Growth and element size
   account for `7 × 80 - 3 × 24 = 488` extra vector bytes. The string comparison
   path copies its long text parameter through the const-reference expression
   operators, while the typed path constructs/moves its predicate directly. That
   offsets one 40-byte allocation, leaving the observed net **+1 call / +448
   bytes**. This is a representation/construction comparison, not an intrinsic
   cost of C++ template checking.

These mechanisms are supported by the measured counts/layouts and current source
in `sql/typed.hpp` and `sql/ast.hpp`; no optimization was silently applied to this
baseline.

## Follow-up

Reserve known selection counts in the typed AST constructors first, then repeat
the same benchmark and retain both result sets. Plain-column projections also
need assessment of the larger `select_item` representation before claiming full
allocation parity. Investigate remaining differences rather than changing the
string baseline or accepting a broader tolerance after measurement.

The incremental migration guide remains a separate M8 deliverable. Any broader
runtime-equivalence claim must also account for decoding as required by the
project performance guidance; this benchmark intentionally stops at SQL and
bound parameters.

## Verification

- `meson test -C buildBenchmark --print-errorlogs`: `Ok: 2`, `Fail: 0`; both
  executables verified 192 SQL/parameter pairs, and the allocation executable
  passed its replacement-allocator calibration.
- `just test`: `Ok: 6`, `Fail: 0`, including PostgreSQL, SQLite, MySQL and the
  typed DSL compile checks.
- `just format-check`: exit 0.
- `just lint` and `meson compile -C buildBenchmark clang-tidy`: both exited 0,
  with warnings. The benchmark build also analyzes the opt-in benchmark sources.
- Source hashes in `metadata.json` still matched the final measured sources;
  all 2,268 retained timing samples were positive.
