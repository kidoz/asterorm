# Typed versus string SQL DSL measurements

Recorded on 2026-11-06T19:12:57.431346+00:00 with Apple M4 Max, macOS-27.2-arm64-arm-64bit-Mach-O.

These measurements cover construction/destruction and SQL/parameter compilation only.
They do not measure decoding, database execution, network I/O, or concurrent use.
Timing uses the standard allocator; a separate executable counts successful C++
`new` calls and requested bytes (not peak RSS, allocator overhead, or direct `malloc`).

3 processes × 21 alternating-order paired samples ×
10000 operations per timing sample. Warmup: 1,000 operations per API/case.
Allocation counting: 256 operations per case, outside all timing measurements.

The predeclared investigation screen is a paired median typed/string ratio >1.10
in every process, or extra allocations/requested bytes. It is not a CI performance gate
or a statistical equivalence proof. Ratio IQR is sample dispersion, not a confidence interval.

| Workload | Compiler dialect | Phase | String ns/op | Typed ns/op | Paired ratio median [IQR] | Allocations string→typed | Bytes string→typed | Investigation signal |
|---|---|---|---:|---:|---|---:|---:|---|
| aggregate | mysql | build | 191.9 | 201.5 | 1.036 [1.013, 1.088] | 8→10 | 1368→1608 | extra allocations; extra bytes |
| aggregate | mysql | build_compile | 507.3 | 508.1 | 1.001 [0.975, 1.027] | 12→14 | 1968→2208 | extra allocations; extra bytes |
| aggregate | mysql | compile | 289.5 | 294.9 | 1.019 [0.995, 1.044] | 4→4 | 600→600 | within screen |
| aggregate | postgres_sqlite | build | 195.1 | 204.1 | 1.044 [1.016, 1.080] | 8→10 | 1368→1608 | extra allocations; extra bytes |
| aggregate | postgres_sqlite | build_compile | 526.8 | 540.0 | 1.023 [0.999, 1.054] | 12→14 | 1968→2208 | extra allocations; extra bytes |
| aggregate | postgres_sqlite | compile | 301.6 | 306.7 | 1.029 [0.995, 1.049] | 4→4 | 600→600 | within screen |
| nullable_projection | mysql | build | 201.5 | 223.5 | 1.111 [1.069, 1.131] | 9→10 | 2536→2984 | timing inconclusive; extra allocations; extra bytes |
| nullable_projection | mysql | build_compile | 710.5 | 723.7 | 1.019 [0.995, 1.040] | 19→20 | 3968→4416 | extra allocations; extra bytes |
| nullable_projection | mysql | compile | 473.2 | 481.9 | 1.022 [0.993, 1.043] | 10→10 | 1432→1432 | within screen |
| nullable_projection | postgres_sqlite | build | 204.2 | 226.4 | 1.099 [1.074, 1.135] | 9→10 | 2536→2984 | extra allocations; extra bytes |
| nullable_projection | postgres_sqlite | build_compile | 730.4 | 742.1 | 1.013 [0.997, 1.028] | 19→20 | 3968→4416 | extra allocations; extra bytes |
| nullable_projection | postgres_sqlite | compile | 488.4 | 493.6 | 1.007 [0.988, 1.037] | 10→10 | 1432→1432 | within screen |
| simple_filter | mysql | build | 62.4 | 90.5 | 1.445 [1.417, 1.482] | 1→3 | 96→168 | repeatable slowdown; extra allocations; extra bytes |
| simple_filter | mysql | build_compile | 303.3 | 330.5 | 1.086 [1.068, 1.104] | 4→6 | 504→576 | extra allocations; extra bytes |
| simple_filter | mysql | compile | 224.1 | 224.0 | 0.998 [0.986, 1.023] | 3→3 | 408→408 | within screen |
| simple_filter | postgres_sqlite | build | 64.4 | 90.2 | 1.414 [1.384, 1.441] | 1→3 | 96→168 | repeatable slowdown; extra allocations; extra bytes |
| simple_filter | postgres_sqlite | build_compile | 319.9 | 351.3 | 1.098 [1.061, 1.122] | 4→6 | 504→576 | extra allocations; extra bytes |
| simple_filter | postgres_sqlite | compile | 247.9 | 246.8 | 0.995 [0.980, 1.024] | 3→3 | 408→408 | within screen |

`postgres_sqlite` is the shared compiler dialect, not a database benchmark.
The compile phase reuses input 0; construction phases vary inputs each iteration.
Object destruction and a common out-of-line observation call are inside each batch.
Do not generalize a within-screen result beyond these workloads and this machine.

See `timings.csv`, `allocations.csv`, and `metadata.json` for raw samples and provenance.
