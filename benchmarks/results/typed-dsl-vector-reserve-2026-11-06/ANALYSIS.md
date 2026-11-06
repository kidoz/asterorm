# Typed DSL vector reservation results

Reserving the known selection counts removed the intermediate vector-growth
allocations. Simple and aggregate construction now match the string baseline's
allocation calls and requested bytes. Nullable plain-column projections still
request more bytes, so the broad unchanged-runtime-cost criterion remains open.

The prior [baseline](../typed-vs-string-dsl-2026-11-06/ANALYSIS.md) is preserved in
commit `0e05907`. The production change adds only two reservations in
`sql/typed.hpp`: the mapped-column count in `make_entity_select_ast` and
`sizeof...(Items)` in `make_projection_select_ast`.

## Same measurement protocol

The benchmark sources, compiler, build options, workload definitions, input
generation, sample counts, and investigation threshold match the baseline. The
only changed measured source hash is `include/asterorm/sql/typed.hpp`.

Apple M4 Max, Apple Clang 21.0.0, C++23, `-O3`, static core, LTO disabled;
three processes, 21 paired samples per process, 10,000 operations per sample.
There are 2,268 timing rows. Allocation counting uses a separate executable and
256 operations per case. Timing uses the standard allocator.

The [generated report](report.md), [timings](timings.csv),
[allocation counts](allocations.csv), [AST layouts](ast_layouts.csv), and
[metadata/source hashes](metadata.json) retain the measurements. Reproduce with
the [same instructions](../../README.md), writing to a new output directory.

## Construction allocation comparison

Counts and bytes below are per query and are identical for both dialect-labelled
construction runs. Bytes are requested C++ `new` bytes, not peak memory or RSS.

| Workload | Typed before: calls / bytes | Typed after: calls / bytes | String: calls / bytes |
|---|---:|---:|---:|
| Simple filter | 3 / 168 | 1 / 96 | 1 / 96 |
| Nullable projection | 10 / 2,984 | 8 / 2,664 | 9 / 2,536 |
| Aggregate | 10 / 1,608 | 8 / 1,368 | 8 / 1,368 |

The simple builder saves two calls and 72 requested bytes. The aggregate builder
saves two calls and 240 bytes. A three-item nullable projection saves two calls
and 320 bytes, with final item capacity three instead of four. Prebuilt-AST
compilation allocation counts and bytes still match the string counterpart for
every workload/dialect; construction-plus-compilation inherits the construction
savings.

## Timing comparison

These are ranges of median paired typed/string ratios across the two compiler
dialect labels. Each sample pairs the APIs within one process. The old and new
executables were measured at different times, so comparing those ratios provides
context rather than a simultaneous before/after experiment.

| Workload | Phase | Before ratio | After ratio |
|---|---|---:|---:|
| Simple filter | Build | 1.414–1.445 | 0.824–0.840 |
| Simple filter | Build + compile | 1.086–1.098 | 0.956–0.957 |
| Nullable projection | Build | 1.099–1.111 | 0.876–0.880 |
| Nullable projection | Build + compile | 1.013–1.019 | 0.923–0.928 |
| Aggregate | Build | 1.036–1.044 | 0.870–0.876 |
| Aggregate | Build + compile | 1.001–1.023 | 0.940–0.940 |

In this run, typed construction was approximately 12–18% faster than the string
path across the tested workloads. Prebuilt compilation ratios were 0.998–1.040
and stayed within the original timing investigation screen. Detailed dispersion
is recorded in the generated report; it is not a confidence interval or portable
performance guarantee. Construction itself is dialect-independent, so the build
labels repeat the same builder workload rather than measure backend differences.

## Remaining scope

Nullable plain projections still use `select_item` rather than the legacy string
column list. On this toolchain, three items request `3 × 80 = 240` vector bytes,
versus `3 × 24 = 72` for the string columns. A 40-byte parameter-copy allocation
in the string comparison path offsets some of that difference, leaving **128
extra requested bytes** in the typed path, despite one fewer allocation. Assess
this representation separately before claiming allocation parity.

The SQL/parameter parity checks pass. Database execution, networking, row decoding,
pool behavior, and compile-time performance remain outside this benchmark. The
migration guide and any broader decoding/equivalence evidence remain separate
deliverables; these reservations do not mark the whole feature contract complete.

## Verification

- `meson compile -C buildBenchmark`: exit 0 in the original optimized configuration.
- `meson test -C buildBenchmark --print-errorlogs`: `Ok: 2`, `Fail: 0`; both
  executables verified 192 SQL/parameter pairs, and allocation calibration passed.
- `python3 benchmarks/run_dsl_benchmark.py --build-root buildBenchmark --output benchmarks/results/typed-dsl-vector-reserve-2026-11-06`:
  completed three processes and retained all 2,268 positive timing samples.
- `just test`: `Ok: 6`, `Fail: 0`, including PostgreSQL, SQLite, MySQL and compile checks.
- `just format-check`: exit 0.
- `just lint` and `meson compile -C buildBenchmark clang-tidy`: both exited 0,
  with warnings.
- Source hashes matched the measured files; compiler/build/machine/OS settings
  matched the baseline, and Git confirmed the committed baseline directory was unchanged.
