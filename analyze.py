"""Validate result comparisons and summarize each PR against its parent."""
import json
import math
from pathlib import Path
import statistics

from native_profiles import summarize

ROOT = Path('/work/peterxcli/datafusion-io-stack-20260916')

def median_elapsed(records):
    return statistics.median(run['elapsed_ms'] for record in records for run in record['measured'])

def equal_cell(a, b, dtype):
    if a is None or b is None:
        return a == b
    if dtype in ('Float32', 'Float64'):
        return math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-12)
    return a == b

def equal_rows(a, b, types):
    if len(a) != len(b):
        return False
    unmatched = list(b)
    for row in a:
        for index, other in enumerate(unmatched):
            if len(row) == len(other) and all(equal_cell(x, y, dtype) for x, y, dtype in zip(row, other, types)):
                unmatched.pop(index)
                break
        else:
            return False
    return True

def main():
    records = []
    for path in sorted((ROOT / 'results').glob('*.json')):
        if path.name.endswith(('-benchmark.json', '-events.json')):
            continue
        record = json.loads(path.read_text())
        if 'measured' in record:
            records.append(record)
    validation = []
    for query in range(43):
        subset = [record for record in records if record['query_id'] == query]
        if not subset:
            continue
        reference = next(record['runs'][0] for record in subset if record['stage'] == 0)
        mismatches = []
        ranking_column = {31: 2, 32: 2, 38: -1, 39: -1, 40: -1, 41: -1}.get(query)
        rank_matches = True
        for record in subset:
            for run in record['runs']:
                assert run['rows'] == reference['rows'], (query, record['stage'], 'row count')
                assert run['rows'] == 0 or run['types'] == reference['types'], (query, record['stage'], 'types')
                if not equal_rows(run['values'], reference['values'], reference['types'] or []):
                    mismatches.append(dict(stage=record['stage'], repeat=record['repeat'], cache=record['cache'], iteration=run['iteration']))
                if ranking_column is not None:
                    rank_matches &= ([row[ranking_column] for row in run['values']] ==
                                     [row[ranking_column] for row in reference['values']])
        expected_nondeterminism = query in [17, 24] or (ranking_column is not None and rank_matches)
        assert not mismatches or expected_nondeterminism, (query, mismatches[:3])
        validation.append(dict(query_id=query, executions=sum(len(record['runs']) for record in subset),
                               multiset_match=not mismatches, mismatch_count=len(mismatches),
                               baseline_mismatch_count=sum(item['stage'] == 0 for item in mismatches),
                               ranking_values_match=rank_matches if ranking_column is not None else None,
                               limitation='SQL has unordered LIMIT or incomplete tie ordering' if mismatches else None))
    timings = []
    for query in range(43):
        for stage in range(6):
            subset = [record for record in records if record['query_id'] == query and record['stage'] == stage and not record['profile']]
            for cache, latency, memory in sorted({(record['cache'], record['latency_ms'], record['memory_limit']) for record in subset}):
                same = [record for record in subset if (record['cache'], record['latency_ms'], record['memory_limit']) == (cache, latency, memory)]
                per_pass = {str(repeat): median_elapsed([record for record in same if record['repeat'] == repeat]) for repeat in sorted({record['repeat'] for record in same})}
                measured = [run for record in same for run in record['measured']]
                timings.append(dict(query_id=query, stage=stage, cache=cache, latency_ms=latency,
                                    memory_limit=memory, elapsed_ms=median_elapsed(same), passes_ms=per_pass,
                                    spill_runs=sum(run['counters'].get('spilled_bytes', 0) > 0 for run in measured),
                                    runs=len(measured), disk_bytes=statistics.median(run['query_io'].get('read_bytes', 0) for run in measured),
                                    cpu_ms=statistics.median(run['cpu_ticks'] * 1000 / same[0]['ticks_per_second'] for run in measured),
                                    read_calls=statistics.median(run['query_io'].get('syscr', 0) for run in measured),
                                    counters={name: statistics.median(run['counters'].get(name, 0) for run in measured) for name in measured[0]['counters']},
                                    pool_peak_bytes=max(record['pool_peak_bytes'] or 0 for record in same),
                                    disk_read_runs=sum(run['query_io'].get('read_bytes', 0) > 0 for run in measured),
                                    background_core_equivalent=statistics.median(max(0, sum(sum(ticks[i] for i in [0, 1, 2, 5, 6, 7]) for ticks in record['selected_cpu_ticks'].values()) / record['ticks_per_second'] - record['process_cpu_seconds']) / record['process_seconds'] for record in same),
                                    peak_rss_kib=max(record['peak_rss_kib'] for record in same)))
    profiles = []
    for path in sorted((ROOT / 'results').glob('profile-*-r0.json')):
        record = json.loads(path.read_text())
        profile = summarize(record, path.with_suffix('.perf.txt'))
        profile['spilled_bytes'] = record['measured'][0]['counters'].get('spilled_bytes', 0)
        profile['native_total_cpu_ms'] = sum(thread['cpu_ms'] for thread in profile['threads'])
        profile['process_cpu_ms'] = record['measured'][0]['cpu_ticks'] * 1000 / record['ticks_per_second']
        assert abs(profile['native_total_cpu_ms'] - profile['process_cpu_ms']) <= max(30, profile['process_cpu_ms'] * 0.05), (path.name, profile['native_total_cpu_ms'], profile['process_cpu_ms'])
        profiles.append(profile)
        path.with_name(path.stem + '-native.json').write_text(json.dumps(profile) + '\n')
    report = dict(timings=timings, validation=validation, profiles=profiles,
                  processes=len(records), executions=sum(len(record['runs']) for record in records))
    (ROOT / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    print(report['processes'], 'processes;', report['executions'], 'query executions')
    print(sum(row['multiset_match'] for row in validation), 'queries match result multisets')

if __name__ == '__main__':
    assert equal_rows([['1.0'], ['2.0']], [['2.0'], ['1.0000000000001']], ['Float64'])
    assert not equal_rows([['a'], ['a']], [['a'], ['b']], ['Utf8'])
    main()
