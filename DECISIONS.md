# Proposed disposition

Keep the first three PRs as opt-in functionality. Retain the shared queue as an experiment for storage latency, and drop the current memory-headroom heuristic from the proposed upstream patch. All five draft PRs remain available for review; these recommendations do not remove the experiments.

| PR | Recommendation | Evidence on lsa-cupid1 |
|---|---|---|
| #4: I/O policy | Keep the explicit choice; preserve the progressive default | Upfront I/O alone changes total warm time by +0.97% / +0.49%, and evicted-cache time by -0.20%. It is not an overall speedup by itself. |
| #5: bounded prefetch | Keep opt-in | Total warm time falls 1.86% / 1.22%; evicted-cache time falls 2.33%. Q29 with 8 ms added per read falls 8.23%. |
| #7: earlier prefetch | Keep | Total warm time falls 0.16% / 0.54%; evicted-cache time falls 2.03%. Q21 with added latency falls 24.09%. |
| #8: shared queue | Defer upstream; continue storage-latency experiments | Warm time increases 0.21% / 0.76%, and evicted-cache time falls only 0.26%. Added-latency controls improve Q21 by 20.00% and Q29 by 5.10%. |
| #9: memory governor | Drop this heuristic from the upstream proposal | Small timing gains do not establish the intended memory benefit. Both fixed and governed queues spill in all six warm Q35 runs at 12G; all versions avoid spilling at 20G. |

All percentages compare each PR with its immediate parent. Warm values are separate reversed-order passes; evicted-cache values use three runs per query. These are observations, not statistical significance claims. The added-latency controls simulate delay per read call and do not establish performance on an actual object store.

The full stack reduces the sum of query medians by 1.82% / 0.88% warm and 5.43% with evicted files. The first three PRs already provide most of that improvement. This supports bounded, earlier prefetch, but does not establish a broad CPU-utilization transformation across ClickBench.

The shared queue makes eight queries more than 5% slower in both warm passes: Q02, Q08, Q24, Q25, Q27, Q39, Q41 and Q43. Some are short queries; use the absolute timing charts alongside these percentages. No query is more than 5% faster in both warm passes from the queue alone.

The governor uses total query-pool headroom, whereas spilling can depend on an individual consumer's fair allowance. A replacement should integrate with that allowance and demonstrate spill reduction under repeated constrained-memory workloads. The current experiment does neither.

The comparisons isolate PRs, not every internal detail: the scheduling yield is bundled with bounded prefetch in #5. No separate speedup is attributed to that yield.
