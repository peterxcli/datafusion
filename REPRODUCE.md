# Reproducing the NVMe experiment

The recorded run is on `lsa-cupid1` in `/work/peterxcli/datafusion-io-stack-20260916`. The complete 100-file partitioned ClickBench dataset has 99,997,497 rows and 14,737,666,736 compressed bytes. [Download the partitioned dataset](https://datasets.clickhouse.com/hits_compatible/athena_partitioned/) using `download.py`. Dataset identity and compiler/binary hashes are recorded in `environment.json` and `dataset-manifest.json`.

| Stage | Source | Commit |
|---|---|---|
| 0 | Fork baseline | `c14976481ea53d7e1806eb9dbc1cff803c6eba31` |
| 1 | [PR #4](https://github.com/peterxcli/datafusion/pull/4) | `b8c7ba40874d57953620d44fe9b14d1011f50940` |
| 2 | [PR #5](https://github.com/peterxcli/datafusion/pull/5) | `263a9bd74f7f7fc5c26e13a82df206da12b2fca9` |
| 3 | [PR #7](https://github.com/peterxcli/datafusion/pull/7) | `f3ebd935ad067b6a5ab453fd8770a87d3733099d` |
| 4 | [PR #8](https://github.com/peterxcli/datafusion/pull/8) | `06901f6fc8c9ecd96be7a61dc9b914a0bef6df93` |
| 5 | [PR #9](https://github.com/peterxcli/datafusion/pull/9) | `c729174d3399df9d41145309221a48adae7b6dd2` |

The `harness/stageN/clickbench.rs` files are the exact instrumented sources used for each binary, verified against the recorded build hashes. They add result capture, query CPU/I/O counters, and opt-in experiment settings. Every version performs the same plan traversal and source cloning. Result formatting and metric extraction occur after the timed query.

To rebuild a version, start with a fresh checkout at the corresponding commit. Copy its `harness/stageN/clickbench.rs` to `benchmarks/src/clickbench.rs`, and the common `harness/clickbench_overlap.rs` to `benchmarks/examples/clickbench_overlap.rs`. Build with Rust 1.97.0:

```sh
CMAKE=/usr/bin/cmake cargo +1.97.0 build --locked --profile release-nonlto \
  -p datafusion-benchmarks --example clickbench_overlap
```

The example uses eight Tokio workers and mimalloc. All runs use eight scan partitions, batch size 8192, a 12G fair memory pool, CPUs 96–111 and NUMA memory node 1. Temporary spill files use the NVMe directory through `TMPDIR`. The pool limit governs tracked reservations; process RSS and file-cache behavior are measured separately.

Stage 0 uses the existing progressive policy. Stages 1–5 set `datafusion.execution.parquet.progressive_io=false`. Stages 2–5 enable 64 MiB per-stream prefetch. Stages 4–5 enable 32 queue jobs / 512 MiB, and stage 5 additionally enables the governor. The runner's `arguments()` function records the complete command for every case.

`run_bench.py` runs the full suite serially: two reversed-order warm passes, three evicted-file passes, Q21/Q29 with 8 ms per read-call latency, Q35 at 20G, and warm native CPU profiles. `cold_profiles.py` adds evicted-file CPU profiles for Q21/Q29/Q35 after the primary run. Completed cases are retained so an interrupted run can resume. Use a new results directory for a fresh experiment.

`prime_cache.py` primes the full dataset on the selected NUMA node before warm measurements. File eviction uses dataset-only `POSIX_FADV_DONTNEED`; the SSD's internal cache remains uncontrolled. Native profiles use per-process `PERF_RECORD_SWITCH` events, with runtime park callbacks disabled. `analyze.py` verifies results and CPU accounting, and `plot_report.py` creates the report.

The measurement scripts retain the original host paths for reproducibility. JSON query IDs and PNG filenames are zero-based; visible chart labels Q01–Q43 are one-based. Thus Q35 is `q34.sql` and `--query 34`.
