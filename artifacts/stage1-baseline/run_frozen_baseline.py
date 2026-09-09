"""Verify a source archive and execute its explicit baseline, serially."""
import argparse
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('evidence')
parser.add_argument('--only', action='append', default=[])
args = parser.parse_args()
root = Path('/dev/shm/mini-ray-validation/project')
evidence = Path(args.evidence)
hashes = json.loads((evidence / 'snapshot.json').read_text())
for name, expected in hashes.items():
    if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
        raise RuntimeError('snapshot drift before validation: ' + name)
manifest = json.loads((root / 'scripts/baseline_manifest.json').read_text())
environment = os.environ.copy()
environment['PYTHONDONTWRITEBYTECODE'] = '1'
environment['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
selections = [('pure', None)] + [('smoke', value) for value in manifest['smoke']]
if args.only:
    allowed = {'pure', *manifest['smoke']}
    if any(value not in allowed for value in args.only):
        raise ValueError('--only must name an exact baseline selection')
    selections = [(mode, value) for mode, value in selections if (value or mode) in args.only]
versions = {'python': sys.version, 'platform': platform.platform(), 'uname': list(platform.uname()),
            'executable': sys.executable, 'packages': {name: metadata.version(name) for name in
              ('pytest', 'cloudpickle', 'iniconfig', 'packaging', 'pluggy', 'pygments')},
            'archive_sha256': hashlib.sha256((evidence / 'project.tar').read_bytes()).hexdigest(),
            'scope': 'fixed pure batch and serial exact POSIX smokes; not the legacy full suite'}
(evidence / 'environment.json').write_text(json.dumps(versions, indent=2) + '\n')
results = []
for index, (mode, selector) in enumerate(selections):
    temporary = Path('/dev/shm/mini-ray-validation/test-temp') / str(index)
    temporary.mkdir(parents=True, exist_ok=True)
    environment['TMPDIR'] = str(temporary)
    command = [sys.executable, str(root / 'scripts/run_baseline.py'), '--' + mode]
    if selector is not None:
        command.append(selector)
    started = time.monotonic()
    print('START', index, selector or 'pure', flush=True)
    # The inner runner owns timeout and process-tree cleanup.
    result = subprocess.run(command, cwd=root, env=environment, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    log = f'{index:02d}-{mode}.log'
    (evidence / log).write_text(result.stdout, encoding='utf-8')
    examples = evidence / 'example-output'
    for output in temporary.rglob('*.py.txt'):
        examples.mkdir(exist_ok=True)
        shutil.copyfile(output, examples / output.name)
    record = {'mode': mode, 'selector': selector, 'command': command,
              'exit_code': result.returncode, 'seconds': round(time.monotonic() - started, 3), 'log': log}
    results.append(record)
    (evidence / 'results.json').write_text(json.dumps(results, indent=2) + '\n', encoding='utf-8')
    print('END', index, result.returncode, record['seconds'], result.stdout[-450:], flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
print('Selected frozen baseline passed.', flush=True)
