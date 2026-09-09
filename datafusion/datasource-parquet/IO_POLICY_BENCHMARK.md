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
