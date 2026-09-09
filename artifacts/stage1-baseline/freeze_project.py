"""Create an immutable source candidate without copying Git or local state."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile

parser = argparse.ArgumentParser()
parser.add_argument('destination')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1] / 'mini-ray'
destination = Path(args.destination).resolve()
destination.mkdir(parents=True, exist_ok=False)
files = []
for directory in ('src', 'tests', 'scripts', 'examples', 'docs', '.github'):
    files.extend(path for path in (root / directory).rglob('*')
                 if path.is_file() and '__pycache__' not in path.parts
                 and not any(part.endswith('.egg-info') for part in path.parts)
                 and path.suffix not in ('.pyc', '.pyo', '.log'))
files.extend(root / name for name in ('README.md', 'pyproject.toml', 'uv.lock',
             'conftest.py', 'LICENSE', '.gitignore'))
files = sorted(set(files))
hashes = {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
          for path in files}
with tarfile.open(destination / 'project.tar', 'w') as archive:
    for path in files:
        name = path.relative_to(root).as_posix()
        info = archive.gettarinfo(str(path), arcname=name)
        info.mtime = 0
        info.uid = info.gid = 0
        info.uname = info.gname = ''
        info.mode = 0o644
        with path.open('rb') as stream:
            archive.addfile(info, stream)
(destination / 'snapshot.json').write_text(json.dumps(hashes, indent=2) + '\n')
digest = hashlib.sha256((destination / 'project.tar').read_bytes()).hexdigest()
(destination / 'archive.sha256').write_text(digest + '  project.tar\n')
print(json.dumps({'directory': str(destination), 'files': len(files), 'sha256': digest}))
