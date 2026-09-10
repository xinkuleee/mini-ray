"""Prepare checked-hash caches for frozen sources; no project imports."""
import hashlib
import json
from pathlib import Path
import py_compile
import sys
import time

name = sys.argv[1]
root = Path('/dev/shm/mini-ray-cleanup-candidates') / name
audit = Path('/mnt/host/c/Users/t-hdong/Desktop/gao/audit/two-version-cleanup/execution') / name
snapshot = json.loads((audit / 'snapshot.json').read_text())
for rel, digest in snapshot.items():
    assert hashlib.sha256((root / rel).read_bytes()).hexdigest() == digest, rel
started = time.monotonic()
caches = {}
for rel in snapshot:
    if rel.startswith('src/') and rel.endswith('.py'):
        path = Path(py_compile.compile(str(root / rel), doraise=True, invalidation_mode=py_compile.PycInvalidationMode.CHECKED_HASH))
        caches[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
for rel, digest in snapshot.items():
    assert hashlib.sha256((root / rel).read_bytes()).hexdigest() == digest, rel
report = {
    'source_archive_sha256': json.loads((audit / 'identity.json').read_text())['archive_sha256'],
    'python': sys.version, 'cache_tag': sys.implementation.cache_tag,
    'mode': 'CHECKED_HASH for frozen project src only; no imports or tests',
    'caches': caches, 'seconds': round(time.monotonic() - started, 3),
    'existing_timeout_seconds': 30, 'source_and_test_bytes_unchanged': True,
    'scope': 'Validation environment cache preparation. Source hashes are rechecked before and after; Python verifies source hash when loading.',
}
out = audit / 'bytecode-preparation.json'
assert not out.exists()
out.write_text(json.dumps(report, indent=2) + chr(10))
print(name, len(caches), report['seconds'], 'checked-hash caches; original inputs unchanged', flush=True)
