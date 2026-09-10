from pathlib import Path
import hashlib
import importlib.metadata
import json
import platform
import subprocess
import sys

audit = Path('/mnt/host/c/Users/t-hdong/Desktop/gao/audit')
base = audit / 'stage2-dependency-repro'
source = audit / 'stage2-validation-03'
root = Path('/dev/shm/mini-ray-enhanced-install-check')
project = root / 'project'
uv = str(root / 'uv-x86_64-unknown-linux-musl/uv')
cache = str(root / 'uv-cache')
python = str(root / 'python/bin/python3.12')
environment_python = str(project / '.venv/bin/python')
expected_archive = 'f3782f572b96233e8f9bae3a7915b8a7f3534904c11f857621df367aa530344a'
assert hashlib.sha256((source / 'project.tar').read_bytes()).hexdigest() == expected_archive
expected = json.loads((source / 'snapshot.json').read_text(encoding='utf-8'))
for name, checksum in expected.items():
    assert hashlib.sha256((project / name).read_bytes()).hexdigest() == checksum, name
commands = [
    [uv, '--version'],
    [uv, 'lock', '--check', '--offline', '--python', python, '--no-python-downloads', '--cache-dir', cache],
    [uv, 'sync', '--frozen', '--extra', 'test', '--python', python, '--no-python-downloads', '--cache-dir', cache],
    [uv, 'pip', 'freeze', '--python', environment_python, '--cache-dir', cache],
    [environment_python, '-c', 'import importlib.metadata as m; import miniray; from miniray import enhanced_publication, enhanced_publication_control; print(miniray.__file__); print(enhanced_publication.__file__); print(enhanced_publication_control.__file__); print(m.metadata("mini-ray")["Requires-Python"]); print(m.version("mini-ray"))'],
]
records = []
for index, command in enumerate(commands):
    print('Running', index, command, flush=True)
    try:
        completed = subprocess.run(command, cwd=project, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=90)
        output = completed.stdout
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        output = str(exc)
        exit_code = 124
    log = 'linux-%02d.txt' % index
    (base / log).write_text(output, encoding='utf-8')
    records.append({'command': command, 'exit_code': exit_code, 'log': log})
    report = {
        'platform': platform.platform(), 'python': sys.version,
        'source_archive_sha256': expected_archive, 'snapshot_files_verified': len(expected),
        'records': records,
        'files': {name: hashlib.sha256((project / name).read_bytes()).hexdigest() for name in ('pyproject.toml', 'uv.lock')},
        'scope': 'Linux exact enhanced source frozen dependency install and imports only; no runtime tests',
    }
    (base / 'linux-results.json').write_text(json.dumps(report, indent=2) + chr(10), encoding='utf-8')
    print(output, flush=True)
    if exit_code:
        raise SystemExit(exit_code)
for name, checksum in expected.items():
    assert hashlib.sha256((project / name).read_bytes()).hexdigest() == checksum, name
report['snapshot_unchanged_after_install'] = True
(base / 'linux-results.json').write_text(json.dumps(report, indent=2) + chr(10), encoding='utf-8')
