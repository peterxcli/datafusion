import html
import json
from pathlib import Path
import statistics
import math

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path('/work/peterxcli/datafusion-io-stack-20260916')
OUT = ROOT / 'report'
NAMES = ['Baseline', 'I/O policy', 'Bounded prefetch', 'Earlier prefetch', 'Shared queue', 'Governor']
COLORS = ['#6f7b91', '#4b9ce2', '#41b6ab', '#f4be35', '#db8562', '#a58ee0']

def delta(after, before):
    return (after / before - 1) * 100

def main():
    OUT.mkdir(exist_ok=True)
    summary = json.loads((ROOT / 'summary.json').read_text())
    prs = json.loads((ROOT / 'prs.json').read_text())
    timings = {(row['query_id'], row['stage'], row['cache'], row['latency_ms'], row['memory_limit']): row
               for row in summary['timings']}
    profiles = {(row['query_id'], row['stage']): row for row in summary['profiles'] if row['cache'] == 'warm'}
    cold_profiles = {(row['query_id'], row['stage']): row for row in summary['profiles'] if row['cache'] == 'cold'}
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})

    aggregates = []
    for stage in range(1, 6):
        warm = []
        for repeat in ['0', '1']:
            parent = sum(timings[q, stage - 1, 'warm', 0, '12G']['passes_ms'][repeat] for q in range(43))
            current = sum(timings[q, stage, 'warm', 0, '12G']['passes_ms'][repeat] for q in range(43))
            warm.append(delta(current, parent))
        cold_parent = sum(timings[q, stage - 1, 'cold', 0, '12G']['elapsed_ms'] for q in range(43))
        cold_current = sum(timings[q, stage, 'cold', 0, '12G']['elapsed_ms'] for q in range(43))
        warm_ratios = [timings[q, stage, 'warm', 0, '12G']['elapsed_ms'] / timings[q, stage - 1, 'warm', 0, '12G']['elapsed_ms'] for q in range(43)]
        consistent_faster = [q + 1 for q in range(43) if all(timings[q, stage, 'warm', 0, '12G']['passes_ms'][repeat] < 0.95 * timings[q, stage - 1, 'warm', 0, '12G']['passes_ms'][repeat] for repeat in ['0', '1'])]
        consistent_slower = [q + 1 for q in range(43) if all(timings[q, stage, 'warm', 0, '12G']['passes_ms'][repeat] > 1.05 * timings[q, stage - 1, 'warm', 0, '12G']['passes_ms'][repeat] for repeat in ['0', '1'])]
        warm_baseline = [delta(sum(timings[q, stage, 'warm', 0, '12G']['passes_ms'][repeat] for q in range(43)), sum(timings[q, 0, 'warm', 0, '12G']['passes_ms'][repeat] for q in range(43))) for repeat in ['0', '1']]
        cold_baseline = delta(cold_current, sum(timings[q, 0, 'cold', 0, '12G']['elapsed_ms'] for q in range(43)))
        aggregates.append(dict(stage=stage, warm_pass_changes=warm, cold_change=delta(cold_current, cold_parent),
                               warm_baseline_pass_changes=warm_baseline, cold_baseline_change=cold_baseline,
                               warm_geomean_change=100 * (math.exp(statistics.mean(math.log(ratio) for ratio in warm_ratios)) - 1),
                               consistent_warm_faster_5pct=consistent_faster, consistent_warm_slower_5pct=consistent_slower,
                               profile_worker_off_cpu_ms=sum(profiles[q, stage]['worker_off_cpu_ms'] for q in range(43)),
                               profile_parent_worker_off_cpu_ms=sum(profiles[q, stage - 1]['worker_off_cpu_ms'] for q in range(43))))
        for parent in dict.fromkeys([stage - 1, 0]):
            folder = OUT / (f'stage{stage}' if parent == stage - 1 else f'baseline-stage{stage}')
            folder.mkdir(exist_ok=True)
            for query in range(43):
                for profile_cache, profile_set in [('warm', profiles), ('cold', cold_profiles)]:
                    if (query, parent) not in profile_set or (query, stage) not in profile_set:
                        continue
                    chart_path = folder / (f'q{query:02d}.png' if profile_cache == 'warm' else f'cold-q{query:02d}.png')
                    if chart_path.exists():
                        continue
                    before = profile_set[query, parent]
                    after = profile_set[query, stage]
                    thread_count = max(sum(t['role'] in ('main', 'worker') or (t['role'] == 'io' and bool(t['intervals_ns'])) for t in p['threads']) for p in [before, after])
                    fig = plt.figure(figsize=(15, max(10, 4 + thread_count * 0.16)), layout='constrained')
                    grid = fig.add_gridspec(2, 2, height_ratios=[1, 2.4])
                    timing_ax = fig.add_subplot(grid[0, 0])
                    categories = ['Warm pass 1', 'Warm pass 2', 'Evicted cache']
                    values = []
                    for candidate in [parent, stage]:
                        row = timings[query, candidate, 'warm', 0, '12G']
                        values.append([row['passes_ms']['0'], row['passes_ms']['1'], timings[query, candidate, 'cold', 0, '12G']['elapsed_ms']])
                    for i, candidate in enumerate([parent, stage]):
                        timing_ax.bar([x + (i - 0.5) * 0.36 for x in range(3)], values[i], width=0.36,
                                      label=NAMES[candidate], color=COLORS[candidate])
                    timing_ax.set_xticks(range(3), categories)
                    timing_ax.set_ylabel('Untraced query median (ms)')
                    timing_ax.legend(fontsize=9)
                    timing_ax.grid(axis='y', alpha=0.2)
                    details = fig.add_subplot(grid[0, 1])
                    details.axis('off')
                    text = [f'Elapsed-time change: {delta(b, a):+.1f}% ({label.lower()})' for a, b, label in zip(values[0], values[1], categories)]
                    for candidate, profile in [(parent, before), (stage, after)]:
                        cold = timings[query, candidate, 'cold', 0, '12G']
                        warm_row = timings[query, candidate, 'warm', 0, '12G']
                        text += ['', NAMES[candidate],
                                 f'Profile: {profile["worker_cpu_ms"]:.1f} worker CPU ms; {profile["worker_off_cpu_ms"]:.1f} worker off-CPU ms',
                                 f'Every worker off CPU: {profile["all_workers_off_cpu_ms"]:.1f} ms',
                                 f'Spilling runs: warm {warm_row["spill_runs"]}/{warm_row["runs"]}; cold {cold["spill_runs"]}/{cold["runs"]}',
                                 f'Cold query disk reads: {cold["disk_bytes"] / 2**20:.0f} MiB']
                        if profile_cache == 'cold':
                            text.append(f'This CPU profile spilled: {profile["spilled_bytes"] / 2**20:.0f} MiB')
                    details.text(0, 1, '\n'.join(text), va='top', fontsize=10)
                    maximum = max(before['elapsed_ms'], after['elapsed_ms'])
                    for column, (candidate, profile) in enumerate([(parent, before), (stage, after)]):
                        ax = fig.add_subplot(grid[1, column])
                        ax.set_facecolor('#202126')
                        workers = sorted((t for t in profile['threads'] if t['role'] == 'worker'), key=lambda t: t['index'])
                        io = sorted((t for t in profile['threads'] if t['role'] == 'io' and t['intervals_ns']), key=lambda t: t['index'])
                        main_thread = [t for t in profile['threads'] if t['role'] == 'main']
                        threads = main_thread + workers + io
                        for row, thread in enumerate(threads):
                            intervals = [(start / 1e6, (stop - start) / 1e6) for start, stop in thread['intervals_ns']]
                            ax.broken_barh(intervals, (row - 0.36, 0.72), facecolors='#e9b500', edgecolors='none')
                        labels = ['Main' if t['role'] == 'main' else f'{"Worker" if t["role"] == "worker" else "I/O"} {t["index"] if t["role"] == "worker" else t["index"] - 8}' for t in threads]
                        ax.set_yticks(range(len(threads)), labels, fontsize=7)
                        ax.set_ylim(thread_count - 0.4, -0.8)
                        ax.axhline(len(main_thread) + len(workers) - 0.5, color='white', alpha=0.2, linewidth=0.7)
                        ax.set_xlim(0, maximum)
                        ax.axvline(profile['elapsed_ms'], color='#d7e0ef', linestyle=':', linewidth=0.8)
                        ax.set_xlabel('Query time (ms), identical scale')
                        ax.set_title(f'{NAMES[candidate]} — {profile_cache} profile {profile["elapsed_ms"]:.1f} ms')
                        ax.grid(axis='x', color='white', alpha=0.08)
                    fig.suptitle(f'Q{query + 1:02d} · {NAMES[parent]} → {NAMES[stage]}\nFull partitioned ClickBench · lsa-cupid1 NVMe · 8 workers / 8 partitions / 12G pool', fontsize=14)
                    fig.supxlabel('Gold = native on-CPU intervals. Off CPU includes sleeping and scheduling delay. Profiles are separate from untraced timings.', fontsize=9)
                    fig.savefig(chart_path, dpi=125)
                    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(14, 13), layout='constrained', sharey=True)
    for ax, cache in zip(axes, ['warm', 'cold']):
        matrix = [[delta(timings[q, stage, cache, 0, '12G']['elapsed_ms'], timings[q, stage - 1, cache, 0, '12G']['elapsed_ms']) for stage in range(1, 6)] for q in range(43)]
        heat = ax.imshow(matrix, cmap='RdYlGn_r', vmin=-25, vmax=25, aspect='auto')
        ax.set_xticks(range(5), NAMES[1:], rotation=25, ha='right')
        ax.set_yticks(range(43), [f'Q{q + 1:02d}' for q in range(43)])
        ax.set_title('Warm: median of six measured iterations' if cache == 'warm' else 'Evicted cache: median of three runs')
        for q in range(43):
            for stage in range(5):
                ax.text(stage, q, f'{matrix[q][stage]:+.1f}%', ha='center', va='center', fontsize=8, color='white' if abs(matrix[q][stage]) > 18 else '#17232b')
    fig.colorbar(heat, ax=axes, fraction=0.025, label='Elapsed time change vs immediate parent (%)')
    fig.suptitle('All 43 queries · each PR vs its parent · negative is faster\nColor range clipped at ±25%; labels retain the actual changes', fontsize=15)
    fig.savefig(OUT / 'overview.png', dpi=140)
    plt.close(fig)

    rows = []
    for item in aggregates:
        stage = item['stage']
        pr = next(pr for pr in prs if pr['stage'] == stage)
        rows.append(f'| [{NAMES[stage]}]({pr["url"]}) | {item["warm_pass_changes"][0]:+.2f}% / {item["warm_pass_changes"][1]:+.2f}% | {item["cold_change"]:+.2f}% |')
    report = f'''# Parquet I/O stack: NVMe comparison

Measured on lsa-cupid1 under `/work/peterxcli/datafusion-io-stack-20260916`. Each stage is compared with its immediate parent; negative elapsed-time changes mean faster.

| PR stage | Warm sum of query medians, pass 1 / pass 2 | Evicted-cache sum of query medians |
|---|---:|---:|
''' + '\n'.join(rows) + f'''

The run contains {summary['executions']:,} query executions in {summary['processes']:,} processes, including warmups and separate profiles. All 43 queries use the complete 100-file dataset: 99,997,497 rows and 14,737,666,736 compressed bytes. File checksums match the previous AWS dataset manifest.

All stages use Rust 1.97.0, the same release profile, timing instrumentation, plan traversal and source cloning, eight Tokio workers, eight scan partitions, and a 12G fair memory pool. Processes and memory are bound to NUMA node 1, CPUs 96–111, where the NVMe device is attached. The full dataset is evicted and then read on NUMA node 1 before profiling and warm timing begin. Warm timings retain two reversed-order passes, each with one excluded warmup and three measured iterations. Cold timings have three one-iteration processes, with dataset-only `POSIX_FADV_DONTNEED` before each. This evicts Linux file pages; it does not clear the SSD's internal cache. All spill files use the same NVMe mount through a task-local `TMPDIR`. Compilation finishes before timing starts.

Additional controls add 8 ms per object-store read call for Q21/Q29 and raise Q35's pool to 20G. These are controls, not measurements of remote storage. Spill counts and query disk-read bytes are retained beside timings. The fixed queue uses 32 jobs / 512 MiB; prefetch uses 64 MiB per stream. The governor comparison changes only its explicit mode.

Native per-process `PERF_RECORD_SWITCH` traces, with the harness's runtime park callbacks disabled, show actual on-CPU intervals, including all eight workers and the runtime's blocking I/O threads. All 43 queries have warm CPU profiles; Q21, Q29, and Q35 additionally have one evicted-cache CPU profile per version. Their wall times are profiled observations, separate from the untraced performance comparison. Off-CPU time includes sleeping and scheduling delay and must not be labeled entirely as I/O wait.

Result comparisons use multisets and floating-point tolerance. {sum(row['multiset_match'] for row in summary['validation'])}/43 queries match across every recorded run. Differences from unordered LIMIT or incomplete tie ordering are listed in `result-validation.json`; six known tied-ranking queries additionally compare ordering values. These exceptions are not claimed as exact result equality.

[Open the per-query before/after charts](index.html). The stack branches contain production changes and tests; this report retains benchmark artifacts separately.
'''
    report += '\n\n## Cumulative change vs fork baseline\n\n| Stage | Warm pass 1 / pass 2 | Evicted cache |\n|---|---:|---:|\n'
    for item in aggregates:
        report += f"| {NAMES[item['stage']]} | {item['warm_baseline_pass_changes'][0]:+.2f}% / {item['warm_baseline_pass_changes'][1]:+.2f}% | {item['cold_baseline_change']:+.2f}% |\n"
    report += '\n\nWarm comparisons exceeding 5% in both passes (a consistency screen, not a significance test):\n\n'
    for item in aggregates:
        report += f"- {NAMES[item['stage']]}: faster {item['consistent_warm_faster_5pct'] or 'none'}; slower {item['consistent_warm_slower_5pct'] or 'none'}. Geometric-mean time change: {item['warm_geomean_change']:+.2f}%.\n"
    report += '\n## Latency and memory controls\n\n| Control | Stage | Median ms | Change vs parent | Spilling measured runs |\n|---|---|---:|---:|---:|\n'
    for query, latency, memory, label in [(20, 8, '12G', 'Q21 +8 ms/read'), (28, 8, '12G', 'Q29 +8 ms/read'), (34, 0, '12G', 'Q35 12G'), (34, 0, '20G', 'Q35 20G')]:
        for stage in range(6):
            row = timings[query, stage, 'warm', latency, memory]
            change = '—' if stage == 0 else f"{delta(row['elapsed_ms'], timings[query, stage - 1, 'warm', latency, memory]['elapsed_ms']):+.2f}%"
            report += f"| {label} | {NAMES[stage]} | {row['elapsed_ms']:.2f} | {change} | {row['spill_runs']}/{row['runs']} |\n"
    report += '\n## Native CPU observations\n\nThese are single profiled observations per query and version, not repeated performance estimates. Worker off-CPU time is summed across eight workers; it includes waiting and scheduling delay.\n\n| Stage | Sum of warm profile wall time (s) | Worker CPU (core-s) | Worker off CPU (core-s) |\n|---|---:|---:|---:|\n'
    for stage in range(6):
        group = [profiles[q, stage] for q in range(43)]
        report += f"| {NAMES[stage]} | {sum(p['elapsed_ms'] for p in group) / 1000:.3f} | {sum(p['worker_cpu_ms'] for p in group) / 1000:.3f} | {sum(p['worker_off_cpu_ms'] for p in group) / 1000:.3f} |\n"
    report += '\nQ19 dominates the queue increase in this single profile pass: its spill volume varies substantially. These observations do not establish a repeatable CPU regression. Repeated untraced timing and spill data remain the performance comparison.\n\n| Q19 profile | Wall ms | Spilled MiB |\n|---|---:|---:|\n'
    for stage in range(6):
        profile = profiles[18, stage]
        report += f"| {NAMES[stage]} | {profile['elapsed_ms']:.2f} | {profile['spilled_bytes'] / 2**20:.2f} |\n"
    if cold_profiles:
        report += '\nEvicted-file CPU profiles confirm physical reads for Q21, Q29 and Q35. Spill variation can dominate these single observations:\n\n| Query | Stage | Profile wall ms | Worker CPU ms | Worker off-CPU ms | Spilled MiB |\n|---|---|---:|---:|---:|---:|\n'
        for (query, stage), profile in sorted(cold_profiles.items()):
            report += f"| Q{query + 1:02d} | {NAMES[stage]} | {profile['elapsed_ms']:.2f} | {profile['worker_cpu_ms']:.2f} | {profile['worker_off_cpu_ms']:.2f} | {profile['spilled_bytes'] / 2**20:.2f} |\n"
    (OUT / 'report.md').write_text(report)
    (OUT / 'profile-summary.json').write_text(json.dumps([{key: value for key, value in row.items() if key != 'threads'} for row in summary['profiles']], indent=2) + '\n')
    (OUT / 'result-validation.json').write_text(json.dumps(summary['validation'], indent=2) + '\n')
    (OUT / 'timings.json').write_text(json.dumps(summary['timings'], indent=2) + '\n')
    (OUT / 'aggregate.json').write_text(json.dumps(aggregates, indent=2) + '\n')
    links = ' · '.join(f'<a href="{html.escape(pr["url"])}">PR #{pr["pr"]}: {NAMES[pr["stage"]]}</a>' for pr in prs)
    options = ''.join(f'<option value="{stage}">{NAMES[stage - 1]} → {NAMES[stage]}</option>' for stage in range(1, 6))
    queries = ''.join(f'<option value="{query}">Q{query + 1:02d}</option>' for query in range(43))
    page = '''<!doctype html><html><head><meta charset="utf-8"><title>Parquet I/O stack · NVMe</title><style>body{font:16px system-ui;margin:24px;background:#edf3f7;color:#142433}h1{font-size:24px}select{font:inherit;padding:8px;margin:8px}img{display:block;width:100%;height:auto;background:white}a{color:#165c96}.note{max-width:1000px;line-height:1.5}</style></head><body><h1>Parquet I/O stack · lsa-cupid1 NVMe</h1><p class="note">Choose each PR’s immediate parent or the fork baseline as the reference. Timing bars use separate untraced runs; gold timelines show native CPU activity. All queries use the full partitioned dataset. Negative timing changes mean faster.</p><p>LINKS</p><label>CPU profile<select id="profile"><option value="warm">Warm files</option><option value="cold" COLD_DISABLED>Evicted files: Q21, Q29, Q35</option></select></label><label>Reference<select id="reference"><option value="parent">Previous PR</option><option value="baseline">Fork baseline</option></select></label><label>Stage<select id="stage">STAGES</select></label><label>Query<select id="query">QUERIES</select></label><a id="download">Open PNG</a><img id="chart" alt="Before and after query timing and native CPU activity"><p><a href="overview.png">All-query overview</a> · <a href="report.md">Report and methodology</a> · <a href="timings.json">Timings</a> · <a href="result-validation.json">Result validation</a></p><script>const s=document.querySelector('#stage'),q=document.querySelector('#query'),r=document.querySelector('#reference'),c=document.querySelector('#profile');function draw(){for(const option of q.options)option.disabled=c.value==='cold'&&!['20','28','34'].includes(option.value);if(q.selectedOptions[0].disabled)q.value='20';const folder=r.value==='baseline'&&s.value!=='1'?`baseline-stage${s.value}`:`stage${s.value}`;const path=`${folder}/${c.value==='cold'?'cold-':''}q${q.value.padStart(2,'0')}.png`;document.querySelector('#chart').src=path;document.querySelector('#download').href=path}const p=new URLSearchParams(location.search);s.value=p.get('stage')||'1';q.value=p.get('query')||'20';r.value=p.get('reference')||'parent';c.value=p.get('profile')||'warm';s.onchange=q.onchange=r.onchange=c.onchange=draw;draw();</script></body></html>'''
    (OUT / 'index.html').write_text(page.replace('LINKS', links).replace('STAGES', options).replace('QUERIES', queries).replace('COLD_DISABLED', '' if len(cold_profiles) == 18 else 'disabled'))
    print('Available PNG charts:', len(list(OUT.glob('**/*.png'))))

if __name__ == '__main__':
    main()
