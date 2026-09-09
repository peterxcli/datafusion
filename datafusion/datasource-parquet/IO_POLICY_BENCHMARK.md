<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Parquet I/O policy experiment

This fork experiment targets DataFusion 55.0.0 so Comet can use the patch through
Cargo git dependencies. Both new options are execution-local `ParquetSource`
builders. Progressive fetching remains the default and prefetch is disabled by
default.

- `with_progressive_io(false)` fetches the output and predicate column chunks
  together when a row group first requests data. Filters still determine which
  rows are returned, but can no longer save the bytes already fetched.
- `with_row_group_prefetch(bytes, memory_pool)` fetches at most one next row group
  while the current reader produces batches. The budget and memory pool bound
  additional compressed bytes; they do not bound current-reader or decoded memory.
  The full group is skipped when it cannot fit. Dropping the scan cancels pending
  work. A speculative read error is retried through the ordinary demand path.

The two controls are configured independently. Prefetch includes columns used only
by row filters and always fetches full chunks. Consequently, progressive fetching
with prefetch also changes the I/O policy for groups successfully prefetched. To
isolate overlap while holding fetched chunks constant, compare upfront fetching
with prefetch off/on. Compare progressive and upfront with prefetch off to measure
the dependent-read versus extra-bytes tradeoff.

## Reproduce

```sh
cargo run --profile release-nonlto -p datafusion-datasource-parquet \
  --example io_policy -- 256 > io-policy.csv
```

The example creates four local Snappy Parquet files in a temporary directory,
using 256 row groups of 131,072 rows and eight Int64 columns per file. That is
33,554,432 rows and 2 GiB of logical values per file. Files are generated and
measured one at a time; generation is outside the timed region.

The six configurations are pushdown off, pushdown with progressive fetching, and
pushdown with upfront fetching, each with prefetch off/on. The prefetch budget is
16 MiB. Every configuration evaluates the same predicate (`c1 < 10000`) and returns
the same columns. Pushdown-off plans retain a physical `FilterExec` before the
output projection. All configurations retain statistics and page-index pruning.

Each configuration runs against both scattered and page-clustered matches, with
and without column/offset indexes, and with either one or seven output columns.
The filter column is omitted from the output in every case. Writer metadata
assertions verify index presence/absence. Expected row counts and checksums are
calculated during generation and checked on every scan.

Round zero warms every configuration. The report uses medians of rounds 1–3,
rotating the order of all six configurations each round. The measured region
includes plan execution, decoding, filtering, and summing every output value.
There is no artificial I/O or downstream delay. These are warm local-file results;
they do not establish Spark end-to-end or remote-storage speedups.

`bytes_scanned` counts requested bytes, including metadata and speculative reads.
`reader_calls` and `reader_ranges` count calls/ranges observed by the reader wrapper,
excluding reads internal to `get_metadata`; they are not physical disk operations
or HTTP request counts. `prefetch_peak` uses DataFusion's existing `PeakRecordingPool` to record peak prefetch reservations. It does not measure current-reader, decoded-buffer, or total process memory. `prefetch_bytes` and `prefetch_row_groups` count completed
speculative reads, including bytes subsequently discarded.

## Measurements (2026-09-09)

Apple M4, 10 CPU cores, 24 GiB RAM, Rust 1.97.0, `release-nonlto`, two Tokio worker threads. Implementation: [`943ddfc20`](https://github.com/peterxcli/datafusion/commit/943ddfc209147ebd8022853482009c2827d77b59). The four files are approximately 1.29–1.32 GB compressed each. All 384 scans across two complete patched runs and all 64 baseline scans passed the independent row-count and checksum assertions, including warmups.

The two tables report milliseconds, each cell a median of three measured scans. `+PF` means a 16 MiB prefetch budget. Both runs are shown because timings moved substantially even though no other build from this experiment ran during measurement. Other desktop activity was uncontrolled. These measurements support the I/O tradeoff; they do not establish a stable overall speedup or zero overhead with the options disabled.

### First run

| Data                          |    Off | Off +PF | Progressive | Progressive +PF | Upfront | Upfront +PF |
| ----------------------------- | -----: | ------: | ----------: | --------------: | ------: | ----------: |
| random, no indexes, wide      | 1856.0 |  1693.7 |      1845.9 |          1646.1 |  1717.3 |      1551.5 |
| random, no indexes, narrow    |  365.9 |   337.5 |       379.3 |           334.8 |   360.2 |       331.5 |
| random, indexes, wide         | 1654.3 |  1548.9 |      1772.4 |          1459.7 |  1586.5 |      1460.5 |
| random, indexes, narrow       |  436.3 |   366.2 |       489.2 |           362.6 |   399.5 |       415.8 |
| clustered, no indexes, wide   | 1807.7 |  1627.7 |       375.3 |           353.2 |   357.0 |       315.4 |
| clustered, no indexes, narrow |  351.3 |   327.5 |       217.3 |           213.2 |   210.5 |       210.8 |
| clustered, indexes, wide      |   46.1 |   117.8 |        48.5 |           118.9 |   132.8 |       116.0 |
| clustered, indexes, narrow    |   21.3 |    35.8 |        25.8 |            39.9 |    38.9 |        39.8 |

### Repeat run

| Data                          |    Off | Off +PF | Progressive | Progressive +PF | Upfront | Upfront +PF |
| ----------------------------- | -----: | ------: | ----------: | --------------: | ------: | ----------: |
| random, no indexes, wide      | 1729.8 |  1499.5 |      1655.6 |          1498.1 |  1642.4 |      1542.2 |
| random, no indexes, narrow    |  416.2 |   371.6 |       404.0 |           347.8 |   411.2 |       371.0 |
| random, indexes, wide         | 1841.6 |  1698.7 |      2374.9 |          1620.4 |  1798.1 |      1638.5 |
| random, indexes, narrow       |  528.4 |   473.3 |       556.2 |           483.0 |   508.3 |       480.6 |
| clustered, no indexes, wide   | 2099.7 |  1916.2 |       450.0 |           466.4 |   445.9 |       462.4 |
| clustered, no indexes, narrow |  369.0 |   353.3 |       298.8 |           275.6 |   245.6 |       255.0 |
| clustered, indexes, wide      |   48.0 |   131.5 |        50.6 |           131.8 |   152.2 |       133.7 |
| clustered, indexes, narrow    |   22.5 |    40.3 |        26.3 |            44.1 |    41.2 |        43.2 |

### Unmodified DataFusion 55 control

The baseline is [`d55523420`](https://github.com/peterxcli/datafusion/commit/d55523420), the fork's `codex/df55-base`. It uses the same benchmark harness, compiler/profile, dependency lock versions, and two-worker runtime, with only the new builder calls removed and the configurations restricted to off/progressive without prefetch. It ran after the first patched matrix and before the repeat. The table compares the progressive configuration without prefetch in all three runs.

| Data                          |  DF 55 | Patched first | Patched repeat |
| ----------------------------- | -----: | ------------: | -------------: |
| random, no indexes, wide      | 1537.6 |        1845.9 |         1655.6 |
| random, no indexes, narrow    |  358.5 |         379.3 |          404.0 |
| random, indexes, wide         | 1763.0 |        1772.4 |         2374.9 |
| random, indexes, narrow       |  380.9 |         489.2 |          556.2 |
| clustered, no indexes, wide   |  308.7 |         375.3 |          450.0 |
| clustered, no indexes, narrow |  210.8 |         217.3 |          298.8 |
| clustered, indexes, wide      |   50.4 |          48.5 |           50.6 |
| clustered, indexes, narrow    |   25.6 |          25.8 |           26.3 |

Several disabled-option controls were slower than unmodified DF 55, and the repeat also moved substantially. This experiment cannot distinguish code overhead from machine/load drift. A stable machine with interleaved baseline/patch trials is required before claiming an end-to-end improvement or promoting defaults.

### Reader I/O and memory

The following counts are from the first run, without prefetch. Byte counts include metadata. Calls and ranges have the wrapper scope described above.

| Data                          | Progressive bytes | Upfront bytes | Progressive calls / ranges | Upfront calls / ranges |
| ----------------------------- | ----------------: | ------------: | -------------------------: | ---------------------: |
| random, no indexes, wide      |     1,309,357,880 | 1,309,357,880 |                 512 / 2048 |             256 / 2048 |
| random, no indexes, narrow    |       303,126,105 |   303,126,105 |                  512 / 512 |              256 / 512 |
| random, indexes, wide         |     1,318,274,373 | 1,318,274,373 |               513 / 229633 |             257 / 2049 |
| random, indexes, narrow       |       312,042,598 |   312,042,598 |                513 / 33025 |              257 / 513 |
| clustered, no indexes, wide   |     1,278,042,437 | 1,278,042,437 |                 512 / 2048 |             256 / 2048 |
| clustered, no indexes, narrow |       271,810,662 |   271,810,662 |                  512 / 512 |              256 / 512 |
| clustered, indexes, wide      |        28,883,141 | 1,286,958,261 |                 513 / 4097 |             257 / 2049 |
| clustered, indexes, narrow    |        13,160,491 |   280,726,486 |                 513 / 1025 |              257 / 513 |

For random matches without indexes, upfront reads reduce 512 reader calls to 256 while fetching the same bytes. With indexes, batching also removes the many small selected-page ranges. For indexed clustered matches, progressive fetching reads only selected pages: upfront reads instead fetch **44.6×** as many bytes for wide output and **21.3×** for narrow output. The wide upfront configurations take approximately 2.4–3.0× the progressive time across these runs.

To isolate overlap, compare **Upfront** with **Upfront +PF**, which fetch the same chunks. For random wide output without indexes, prefetch reduced the median from 1717.3 to 1551.5 ms (9.7%) in the first run and from 1642.4 to 1542.2 ms (6.1%) in the repeat. This is evidence of potential overlap benefit in this local workload, with the baseline and timing caveats above. Some cases regress: clustered wide output without indexes improved by 11.6% in the first run but regressed by 3.7% in the repeat.

All groups fit the 16 MiB prefetch budget in this fixture; 255 future groups were prefetched. Peak compressed prefetch reservations were about 5.1 MB for wide output and 1.1 MB for narrow output. These are exact reservation peaks from `PeakRecordingPool`, not total reader/process memory. Upfront demand allocations are outside that prefetch reservation.

The full-chunk prefetch strategy loses much of page pruning's byte savings even when progressive demand reads are selected. Both options therefore remain opt-in. A remote-object-store benchmark with real latency, bandwidth limits, backend request counters, and concurrent scans is still needed before an upstream performance claim.
