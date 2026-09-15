import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess

root = Path('/work/peterxcli/datafusion-io-stack-20260916')
manifest = json.loads((root / 'dataset-manifest.json').read_text())
(root / 'input').mkdir(exist_ok=True)

def download(entry):
    target = root / 'input' / entry['file']
    if not target.exists():
        temp = target.with_suffix('.partial')
        subprocess.run(['curl', '--fail', '--location', '--silent', '--show-error',
                        '--retry', '5', '--output', str(temp),
                        'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/' + entry['file']], check=True)
        temp.rename(target)
    with target.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    assert target.stat().st_size == entry['bytes'], target
    assert digest == entry['sha256'], target
    print(entry['file'], 'verified', flush=True)

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    list(executor.map(download, manifest))
(root / 'dataset.ready').write_text(f'{len(manifest)} verified files\n')
