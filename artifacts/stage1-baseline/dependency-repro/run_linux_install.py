from pathlib import Path
import hashlib
import json
import platform
import subprocess
import sys

base = Path('/mnt/host/c/Users/t-hdong/Desktop/gao/audit/stage1-dependency-repro')
root = Path('/dev/shm/mini-ray-install-check')
project = root / 'project'
uv = str(root / 'uv-x86_64-unknown-linux-musl/uv')
cache = str(root / 'uv-cache')
python = str(root / 'python/bin/python3.12')
environment_python = str(project / '.venv/bin/python')
commands = [
    [uv, '--version'],
    [uv, 'lock', '--check', '--offline', '--python', python, '--no-python-downloads', '--cache-dir', cache],
    [uv, 'sync', '--frozen', '--extra', 'test', '--python', python, '--no-python-downloads', '--cache-dir', cache],
    [uv, 'pip', 'freeze', '--python', environment_python, '--cache-dir', cache],
    [environment_python, '-c', 'import importlib.metadata as m; import miniray; print(miniray.__file__); print(m.metadata("mini-ray")["Requires-Python"]); print(m.version("mini-ray"))'],
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
    log = 'linux-%02d.log' % index
    (base / log).write_text(output, encoding='utf-8')
    records.append({'command': command, 'exit_code': exit_code, 'log': log})
    report = {'platform': platform.platform(), 'python': sys.version, 'records': records, 'files': {name: hashlib.sha256((project / name).read_bytes()).hexdigest() for name in ('pyproject.toml', 'uv.lock')}, 'scope': 'Linux frozen dependency install and import only; no runtime tests'}
    (base / 'linux-results.json').write_text(json.dumps(report, indent=2) + chr(10), encoding='utf-8')
    print(output, flush=True)
    if exit_code:
        raise SystemExit(exit_code)
