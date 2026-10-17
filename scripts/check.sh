#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -x .tools/cargo/bin/cargo ]; then
    export RUSTUP_HOME="$PWD/.tools/rustup" CARGO_HOME="$PWD/.tools/cargo"
    export PATH="$CARGO_HOME/bin:$PATH"
fi
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace -- --test-threads=2
python3 -m unittest discover -s bench -p 'test_*.py'
if [ "${RUNSPK_TEST_DOCKER:-0}" = 1 ]; then
    cargo test --locked --test advanced --test container_cleanup -- --ignored --test-threads=1
fi
