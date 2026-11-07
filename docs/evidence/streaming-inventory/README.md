# Streaming inventory evidence

This is the pre-implementation snapshot supporting the
[streaming contract](../../streaming-design.md). It is not a production API test.

From the repository root with PostgreSQL-enabled `buildDir` built and the existing
test service running:

```sh
python3 docs/evidence/streaming-inventory/capture.py --build-root buildDir
```

`ASTERORM_TEST_CONNINFO` or the existing Compose defaults select the server; values
are never printed or recorded. No persistent tables/schema changes are made.
The probe uses read-only generated data, transaction-local cursors and a native
cancel request. It is synchronous and does not enable pipeline mode.

- `probe.cpp` checks the original repository stream signature is absent and the
  current result factory rejects single-tuple results, then observes native
  single-row completion, late SQL failure, cursor requirements/batches/cleanup
  and cancel/drain/reuse. These absence assertions intentionally freeze the
  pre-implementation baseline; preserve this capture after the APIs are added.
- `results.json` contains native protocol results and scans. Buffered, single-row
  and cursor scans run in separate processes, each consuming 100,000 rows with
  a 1,024-byte payload. `getrusage` peak RSS is normalized to bytes (Darwin reports
  bytes; Linux reports KiB). Growth is the final peak minus the post-connect peak.
  No collection of decoded entities is retained. The capture enforces max batches
  of 1/256 and the proposed 32 MiB growth budget for the two native streaming scans.
- `toolchain.json` records the exact coroutine and `<generator>` source/diagnostics
  compiled with the configured project's C++23 flags. Coroutine compile succeeds;
  generator compile fails on the reviewed host because the header is unavailable.
  This expected finding does not fail the capture. Other toolchains require their
  own feature probe; the host result is not a claim about all Clang/libstdc++ builds.
- `metadata.json` binds evidence and inspected source hashes, compiler/platform,
  Git base and exit codes. It is unsigned local evidence, not CI/release provenance.

The script overwrites its capture files. Preserve this snapshot as historical
evidence after implementation; use a new destination/copy for later experiments.
The configured command includes local include/library paths, not connection
strings or credentials. Server/client versions are recorded separately.

Native batching does not prove production session-busy rejection, lease lifetime,
per-row hydration, observer behavior, destructor cancellation or repository RSS.
Those remain explicit runtime acceptance cases in the contract. The large-row
case, other platforms and server execution-plan memory are not measured here.
