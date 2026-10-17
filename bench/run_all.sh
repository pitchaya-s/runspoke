#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -x .tools/cargo/bin/cargo ]; then
    export RUSTUP_HOME="$PWD/.tools/rustup" CARGO_HOME="$PWD/.tools/cargo"
    export PATH="$CARGO_HOME/bin:$PATH"
fi
task_python="$PWD/.venv/bin/python"
if [ ! -x "$task_python" ]; then
    echo 'Create .venv and install bench/requirements.txt first.' >&2
    exit 1
fi
cargo build --release --workspace
"$task_python" bench/report.py --capture
for seed in 42 7 1337; do
    target/release/runspk-bench --seed "$seed" --jobs 120 --out "bench/results/seed-$seed"
done
"$task_python" bench/run_execution.py --seed 42
"$task_python" bench/report.py
