# Typed versus string SQL DSL measurements

Recorded on 2026-11-06T19:41:31.012622+00:00 with Apple M4 Max, macOS-27.2-arm64-arm-64bit-Mach-O.

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
| aggregate | mysql | build | 191.5 | 165.2 | 0.862 [0.847, 0.878] | 8→8 | 1368→1368 | within screen |
| aggregate | mysql | build_compile | 513.5 | 478.3 | 0.930 [0.919, 0.942] | 12→12 | 1968→1968 | within screen |
| aggregate | mysql | compile | 291.6 | 300.8 | 1.035 [1.015, 1.046] | 4→4 | 600→600 | within screen |
| aggregate | postgres_sqlite | build | 193.2 | 168.2 | 0.871 [0.857, 0.888] | 8→8 | 1368→1368 | within screen |
| aggregate | postgres_sqlite | build_compile | 530.9 | 494.4 | 0.931 [0.921, 0.946] | 12→12 | 1968→1968 | within screen |
| aggregate | postgres_sqlite | compile | 303.4 | 314.9 | 1.038 [1.030, 1.051] | 4→4 | 600→600 | within screen |
| nullable_projection | mysql | build | 202.0 | 168.4 | 0.829 [0.819, 0.842] | 9→8 | 2536→2496 | within screen |
| nullable_projection | mysql | build_compile | 722.3 | 658.8 | 0.912 [0.901, 0.927] | 19→18 | 3968→3928 | within screen |
| nullable_projection | mysql | compile | 468.9 | 470.8 | 1.004 [0.991, 1.012] | 10→10 | 1432→1432 | within screen |
| nullable_projection | postgres_sqlite | build | 205.9 | 172.1 | 0.833 [0.823, 0.849] | 9→8 | 2536→2496 | within screen |
| nullable_projection | postgres_sqlite | build_compile | 748.2 | 681.5 | 0.910 [0.898, 0.927] | 19→18 | 3968→3928 | within screen |
| nullable_projection | postgres_sqlite | compile | 490.8 | 489.9 | 1.002 [0.985, 1.011] | 10→10 | 1432→1432 | within screen |
| simple_filter | mysql | build | 64.0 | 53.5 | 0.839 [0.822, 0.855] | 1→1 | 96→96 | within screen |
| simple_filter | mysql | build_compile | 306.4 | 290.9 | 0.949 [0.941, 0.972] | 4→4 | 504→504 | within screen |
| simple_filter | mysql | compile | 229.5 | 231.9 | 1.000 [0.991, 1.010] | 3→3 | 408→408 | within screen |
| simple_filter | postgres_sqlite | build | 64.5 | 53.5 | 0.833 [0.820, 0.839] | 1→1 | 96→96 | within screen |
| simple_filter | postgres_sqlite | build_compile | 320.3 | 310.7 | 0.968 [0.955, 0.974] | 4→4 | 504→504 | within screen |
| simple_filter | postgres_sqlite | compile | 250.0 | 249.3 | 1.001 [0.995, 1.016] | 3→3 | 408→408 | within screen |

`postgres_sqlite` is the shared compiler dialect, not a database benchmark.
The compile phase reuses input 0; construction phases vary inputs each iteration.
Object destruction and a common out-of-line observation call are inside each batch.
Do not generalize a within-screen result beyond these workloads and this machine.

See `timings.csv`, `allocations.csv`, and `metadata.json` for raw samples and provenance.
