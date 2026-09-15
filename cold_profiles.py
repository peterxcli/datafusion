import json
from run_bench import run, ROOT

for query in [20, 28, 34]:
    for stage in range(6):
        run(stage, query, 0, cache='cold', profile=True)
        path = ROOT / 'results' / f'profile-cold-q{query:02d}-s{stage}-lat0-12G-r0.json'
        record = json.loads(path.read_text())
        disk_bytes = record['measured'][0]['query_io']['read_bytes']
        assert disk_bytes > 0, (query, stage, 'Evicted profile performed no physical reads')
        print('Verified physical reads:', query + 1, stage, disk_bytes, flush=True)
(ROOT / 'cold-profiles.done').write_text('Q21, Q29, Q35; all six versions; one evicted-cache native profile each\n')
