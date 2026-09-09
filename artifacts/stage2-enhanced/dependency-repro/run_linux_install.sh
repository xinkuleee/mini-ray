set -eu
base=/mnt/host/c/Users/t-hdong/Desktop/gao/audit
target=/dev/shm/mini-ray-enhanced-install-check
mkdir -p "$target/project"
tar -xf "$base/linux-validation-runtime-executable.tar" -C "$target"
tar -xf "$base/stage2-validation-03/project.tar" -C "$target/project"
tar -xzf "$base/stage1-dependency-repro/uv-0.11.26-linux-musl.tar.gz" -C "$target"
exec "$target/python/bin/python3.12" "$base/stage2-dependency-repro/run_linux_install.py"
