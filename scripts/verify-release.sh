#!/usr/bin/env bash
# Verify the same release artifact locally and on each supported CI runner.
set -euo pipefail
if [ "$#" != 1 ]; then
    printf 'Usage: %s ARCHIVE\n' "$0" >&2
    exit 2
fi
verify_archive=$(realpath "$1")
cd "$(dirname "$0")/.."
if [ -x .tools/cargo/bin/cargo ]; then
    export RUSTUP_HOME="$PWD/.tools/rustup" CARGO_HOME="$PWD/.tools/cargo"
    export PATH="$CARGO_HOME/bin:$PATH"
fi
verify_stage=$(mktemp -d)
trap 'rm -rf "$verify_stage"' EXIT
(
    cd "$(dirname "$verify_archive")"
    sha256sum --check --strict "$(basename "$verify_archive").sha256"
)
scripts/install.sh "$verify_archive" "$verify_stage/bin"
# Reinstall while an old inode remains open, as happens with running executables.
exec 3< "$verify_stage/bin/runspk"
scripts/install.sh "$verify_archive" "$verify_stage/bin"
cmp /proc/self/fd/3 "$verify_stage/bin/runspk"
exec 3<&-
tar -xzf "$verify_archive" -C "$verify_stage"
"$verify_stage/bin/runspk" completions bash > "$verify_stage/completion"
test -s "$verify_stage/completion"
RUNSPK_PRODUCT_BIN_DIR="$verify_stage/bin" RUNSPK_PRODUCT_EXAMPLE_DIR="$verify_stage/examples/python-project" \
    cargo test --locked --test product -- --nocapture
