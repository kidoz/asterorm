# Typed versus string SQL DSL measurements

Recorded on 2026-11-06T19:30:05.788209+00:00 with Apple M4 Max, macOS-27.2-arm64-arm-64bit-Mach-O.

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
| aggregate | mysql | build | 184.0 | 161.1 | 0.876 [0.858, 0.897] | 8→8 | 1368→1368 | within screen |
| aggregate | mysql | build_compile | 487.8 | 459.1 | 0.940 [0.923, 0.959] | 12→12 | 1968→1968 | within screen |
| aggregate | mysql | compile | 286.1 | 291.0 | 1.021 [0.999, 1.050] | 4→4 | 600→600 | within screen |
| aggregate | postgres_sqlite | build | 185.6 | 161.7 | 0.870 [0.844, 0.908] | 8→8 | 1368→1368 | within screen |
| aggregate | postgres_sqlite | build_compile | 508.4 | 478.8 | 0.940 [0.923, 0.955] | 12→12 | 1968→1968 | within screen |
| aggregate | postgres_sqlite | compile | 295.9 | 307.6 | 1.040 [0.996, 1.069] | 4→4 | 600→600 | within screen |
| nullable_projection | mysql | build | 198.2 | 176.1 | 0.880 [0.864, 0.911] | 9→8 | 2536→2664 | extra bytes |
| nullable_projection | mysql | build_compile | 695.4 | 636.9 | 0.923 [0.907, 0.932] | 19→18 | 3968→4096 | extra bytes |
| nullable_projection | mysql | compile | 454.1 | 459.7 | 1.015 [0.988, 1.036] | 10→10 | 1432→1432 | within screen |
| nullable_projection | postgres_sqlite | build | 197.9 | 173.8 | 0.876 [0.860, 0.894] | 9→8 | 2536→2664 | extra bytes |
| nullable_projection | postgres_sqlite | build_compile | 719.9 | 666.5 | 0.928 [0.912, 0.939] | 19→18 | 3968→4096 | extra bytes |
| nullable_projection | postgres_sqlite | compile | 470.1 | 480.2 | 1.019 [0.990, 1.045] | 10→10 | 1432→1432 | within screen |
| simple_filter | mysql | build | 61.6 | 51.5 | 0.840 [0.812, 0.860] | 1→1 | 96→96 | within screen |
| simple_filter | mysql | build_compile | 293.2 | 282.5 | 0.956 [0.933, 0.986] | 4→4 | 504→504 | within screen |
| simple_filter | mysql | compile | 219.6 | 220.4 | 0.998 [0.979, 1.032] | 3→3 | 408→408 | within screen |
| simple_filter | postgres_sqlite | build | 63.1 | 52.2 | 0.824 [0.809, 0.842] | 1→1 | 96→96 | within screen |
| simple_filter | postgres_sqlite | build_compile | 312.0 | 298.8 | 0.957 [0.931, 0.978] | 4→4 | 504→504 | within screen |
| simple_filter | postgres_sqlite | compile | 240.4 | 240.4 | 1.001 [0.972, 1.028] | 3→3 | 408→408 | within screen |

`postgres_sqlite` is the shared compiler dialect, not a database benchmark.
The compile phase reuses input 0; construction phases vary inputs each iteration.
Object destruction and a common out-of-line observation call are inside each batch.
Do not generalize a within-screen result beyond these workloads and this machine.

See `timings.csv`, `allocations.csv`, and `metadata.json` for raw samples and provenance.
