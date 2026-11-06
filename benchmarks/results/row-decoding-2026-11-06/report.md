# Typed versus string projection row decoding

Machine: Apple M4 Max, macOS-27.2-arm64-arm-64bit-Mach-O; started 2026-11-06T20:08:00.760174+00:00.

Actual production per-row helpers and native backend result getters are measured.
Fixtures are synthetic in-memory text results, not executed SQL queries.
Timed work includes decoding/destruction, plus vector reserve/append/destruction
in `result_vector`. SQL construction, execution, fixture creation and verification
are outside measurement. Timing and C++ allocation counting use separate executables.

3 processes × 21 paired samples per case; target
8192 decoded rows per batch, at least one whole result set.
Predeclared investigation: paired median ratio >1.10 in every process, or extra
allocation calls/requested bytes. IQR is dispersion, not a confidence interval.

| Backend | Shape | Rows/result | Phase | String ns/row | Typed ns/row | Paired ratio [IQR] | Calls/row string→typed | Bytes/row string→typed | Signal |
|---|---|---:|---|---:|---:|---|---:|---:|---|
| mysql | aggregates | 1 | result_vector | 83.1 | 82.2 | 0.987 [0.964, 1.001] | 3.000000→3.000000 | 128.0000→128.0000 | within screen |
| mysql | aggregates | 1 | row_decode | 71.6 | 70.4 | 0.970 [0.963, 0.988] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| mysql | aggregates | 256 | result_vector | 71.9 | 69.3 | 0.961 [0.951, 0.980] | 2.003906→2.003906 | 128.0000→128.0000 | within screen |
| mysql | aggregates | 256 | row_decode | 70.3 | 68.3 | 0.976 [0.958, 0.991] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| mysql | aggregates | 16384 | result_vector | 75.8 | 72.7 | 0.958 [0.940, 0.971] | 2.000061→2.000061 | 128.0000→128.0000 | within screen |
| mysql | aggregates | 16384 | row_decode | 71.9 | 68.9 | 0.962 [0.947, 0.979] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| mysql | nullable | 1 | result_vector | 27.3 | 25.5 | 0.918 [0.908, 0.945] | 1.000000→1.000000 | 72.0000→72.0000 | within screen |
| mysql | nullable | 1 | row_decode | 15.5 | 13.1 | 0.843 [0.817, 0.852] | 0.000000→0.000000 | 0.0000→0.0000 | within screen |
| mysql | nullable | 256 | result_vector | 50.4 | 43.2 | 0.857 [0.849, 0.866] | 0.503906→0.503906 | 100.0000→100.0000 | within screen |
| mysql | nullable | 256 | row_decode | 47.7 | 41.3 | 0.861 [0.855, 0.872] | 0.500000→0.500000 | 28.0000→28.0000 | within screen |
| mysql | nullable | 16384 | result_vector | 51.6 | 44.6 | 0.859 [0.850, 0.874] | 0.500061→0.500061 | 100.0000→100.0000 | within screen |
| mysql | nullable | 16384 | row_decode | 47.9 | 41.5 | 0.863 [0.854, 0.877] | 0.500000→0.500000 | 28.0000→28.0000 | within screen |
| mysql | scalars | 1 | result_vector | 67.3 | 66.6 | 0.989 [0.974, 1.009] | 3.000000→3.000000 | 160.0000→160.0000 | within screen |
| mysql | scalars | 1 | row_decode | 54.1 | 53.1 | 0.974 [0.964, 0.984] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| mysql | scalars | 256 | result_vector | 68.4 | 63.0 | 0.928 [0.916, 0.942] | 2.003906→2.003906 | 160.0000→160.0000 | within screen |
| mysql | scalars | 256 | row_decode | 67.9 | 63.4 | 0.935 [0.928, 0.950] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| mysql | scalars | 16384 | result_vector | 71.5 | 67.4 | 0.947 [0.938, 0.955] | 2.000061→2.000061 | 160.0000→160.0000 | within screen |
| mysql | scalars | 16384 | row_decode | 69.4 | 65.4 | 0.946 [0.936, 0.953] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| postgres | aggregates | 1 | result_vector | 123.7 | 121.9 | 0.987 [0.966, 1.005] | 3.000000→3.000000 | 128.0000→128.0000 | within screen |
| postgres | aggregates | 1 | row_decode | 110.4 | 108.4 | 0.985 [0.968, 1.001] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| postgres | aggregates | 256 | result_vector | 109.6 | 107.9 | 0.986 [0.969, 0.999] | 2.003906→2.003906 | 128.0000→128.0000 | within screen |
| postgres | aggregates | 256 | row_decode | 107.8 | 106.0 | 0.987 [0.976, 0.996] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| postgres | aggregates | 16384 | result_vector | 113.9 | 111.2 | 0.980 [0.961, 1.001] | 2.000061→2.000061 | 128.0000→128.0000 | within screen |
| postgres | aggregates | 16384 | row_decode | 109.0 | 106.8 | 0.981 [0.965, 0.995] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| postgres | nullable | 1 | result_vector | 40.9 | 37.9 | 0.930 [0.904, 0.958] | 1.000000→1.000000 | 72.0000→72.0000 | within screen |
| postgres | nullable | 1 | row_decode | 26.5 | 25.1 | 0.932 [0.920, 0.983] | 0.000000→0.000000 | 0.0000→0.0000 | within screen |
| postgres | nullable | 256 | result_vector | 70.2 | 66.0 | 0.945 [0.933, 0.958] | 0.503906→0.503906 | 100.0000→100.0000 | within screen |
| postgres | nullable | 256 | row_decode | 71.1 | 67.9 | 0.954 [0.942, 0.963] | 0.500000→0.500000 | 28.0000→28.0000 | within screen |
| postgres | nullable | 16384 | result_vector | 73.4 | 69.8 | 0.952 [0.934, 0.963] | 0.500061→0.500061 | 100.0000→100.0000 | within screen |
| postgres | nullable | 16384 | row_decode | 74.0 | 70.3 | 0.957 [0.939, 0.970] | 0.500000→0.500000 | 28.0000→28.0000 | within screen |
| postgres | scalars | 1 | result_vector | 100.7 | 100.1 | 1.000 [0.980, 1.015] | 3.000000→3.000000 | 160.0000→160.0000 | within screen |
| postgres | scalars | 1 | row_decode | 86.2 | 86.1 | 1.001 [0.977, 1.025] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| postgres | scalars | 256 | result_vector | 95.8 | 94.4 | 0.995 [0.977, 1.007] | 2.003906→2.003906 | 160.0000→160.0000 | within screen |
| postgres | scalars | 256 | row_decode | 96.2 | 94.2 | 0.984 [0.966, 1.000] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| postgres | scalars | 16384 | result_vector | 100.1 | 99.6 | 0.994 [0.987, 1.006] | 2.000061→2.000061 | 160.0000→160.0000 | within screen |
| postgres | scalars | 16384 | row_decode | 98.0 | 97.4 | 0.991 [0.978, 1.002] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| sqlite | aggregates | 1 | result_vector | 80.5 | 80.8 | 1.007 [0.990, 1.017] | 3.000000→3.000000 | 128.0000→128.0000 | within screen |
| sqlite | aggregates | 1 | row_decode | 69.7 | 68.0 | 0.981 [0.968, 0.994] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| sqlite | aggregates | 256 | result_vector | 71.9 | 68.8 | 0.953 [0.948, 0.969] | 2.003906→2.003906 | 128.0000→128.0000 | within screen |
| sqlite | aggregates | 256 | row_decode | 70.0 | 66.8 | 0.957 [0.942, 0.966] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| sqlite | aggregates | 16384 | result_vector | 75.7 | 72.3 | 0.951 [0.932, 0.966] | 2.000061→2.000061 | 128.0000→128.0000 | within screen |
| sqlite | aggregates | 16384 | row_decode | 70.4 | 67.9 | 0.961 [0.944, 0.969] | 2.000000→2.000000 | 80.0000→80.0000 | within screen |
| sqlite | nullable | 1 | result_vector | 28.8 | 26.0 | 0.890 [0.874, 0.914] | 1.000000→1.000000 | 72.0000→72.0000 | within screen |
| sqlite | nullable | 1 | row_decode | 15.5 | 12.9 | 0.835 [0.820, 0.848] | 0.000000→0.000000 | 0.0000→0.0000 | within screen |
| sqlite | nullable | 256 | result_vector | 51.3 | 43.5 | 0.849 [0.835, 0.861] | 0.503906→0.503906 | 100.0000→100.0000 | within screen |
| sqlite | nullable | 256 | row_decode | 48.2 | 42.0 | 0.872 [0.854, 0.888] | 0.500000→0.500000 | 28.0000→28.0000 | within screen |
| sqlite | nullable | 16384 | result_vector | 51.9 | 45.4 | 0.875 [0.859, 0.888] | 0.500061→0.500061 | 100.0000→100.0000 | within screen |
| sqlite | nullable | 16384 | row_decode | 48.7 | 42.1 | 0.865 [0.849, 0.877] | 0.500000→0.500000 | 28.0000→28.0000 | within screen |
| sqlite | scalars | 1 | result_vector | 67.5 | 66.8 | 0.997 [0.971, 1.009] | 3.000000→3.000000 | 160.0000→160.0000 | within screen |
| sqlite | scalars | 1 | row_decode | 54.6 | 53.1 | 0.975 [0.962, 0.982] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| sqlite | scalars | 256 | result_vector | 67.9 | 63.9 | 0.942 [0.932, 0.955] | 2.003906→2.003906 | 160.0000→160.0000 | within screen |
| sqlite | scalars | 256 | row_decode | 67.0 | 63.8 | 0.952 [0.943, 0.964] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |
| sqlite | scalars | 16384 | result_vector | 72.2 | 68.7 | 0.946 [0.935, 0.959] | 2.000061→2.000061 | 160.0000→160.0000 | within screen |
| sqlite | scalars | 16384 | row_decode | 68.6 | 65.0 | 0.946 [0.933, 0.959] | 2.000000→2.000000 | 112.0000→112.0000 | within screen |

Raw samples, correctness verification and build/source provenance are retained.
These results do not establish server, I/O, full-query or portable runtime equivalence.
