#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -x .tools/cargo/bin/cargo ]; then
    export RUSTUP_HOME="$PWD/.tools/rustup" CARGO_HOME="$PWD/.tools/cargo"
    export PATH="$CARGO_HOME/bin:$PATH"
fi
[ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] || {
    printf 'Packaging supports Linux x86_64 only\n' >&2; exit 1;
}
getconf GNU_LIBC_VERSION | grep -q '^glibc ' || { printf 'Packaging requires glibc\n' >&2; exit 1; }
# Retain caller flags while removing build-machine identities from Rust diagnostics.
if [ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ]; then
    IFS=$'\x1f' read -r -a package_flags <<< "$CARGO_ENCODED_RUSTFLAGS"
else
    read -r -a package_flags <<< "${RUSTFLAGS:-}"
fi
package_flags+=("--remap-path-prefix=$HOME=/build/user" "--remap-path-prefix=$PWD=/build/runspk")
printf -v CARGO_ENCODED_RUSTFLAGS '%s\x1f' "${package_flags[@]}"
export CARGO_ENCODED_RUSTFLAGS=${CARGO_ENCODED_RUSTFLAGS%$'\x1f'}
cargo build --release --locked --workspace --bins --target x86_64-unknown-linux-gnu --target-dir target -j 2
package_bin=target/x86_64-unknown-linux-gnu/release
package_reported=$(timeout --signal=KILL --foreground 5 "$package_bin/runspk" --version)
package_version=${package_reported#runspk }
[[ "$package_reported" = "runspk $package_version" && "$package_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.+-]+)?$ ]] || {
    printf 'Invalid runspk --version: %s\n' "$package_reported" >&2; exit 1;
}
package_out=${1:-dist}
package_out=$(realpath -m "$package_out")
package_stage=$(mktemp -d)
trap 'rm -rf "$package_stage"' EXIT
for binary in runspk runspk-scheduler runspk-worker runspk-bench; do
    install -m 755 "$package_bin/$binary" "$package_stage/$binary"
    for package_prefix in "$HOME" "$PWD"; do
        if LC_ALL=C grep -aFq -- "$package_prefix/" "$package_stage/$binary"; then
            printf '%s contains a local build path; refusing to package\n' "$binary" >&2
            exit 1
        fi
    done
done
install -m 755 scripts/install.sh "$package_stage/install.sh"
package_files=(
    LICENSE README.md CHANGELOG.md
    docs/ARCHITECTURE.md docs/FAILURE-MODEL.md docs/SCHEDULING.md docs/DEPLOYMENT.md
    docs/WORKFLOWS.md docs/QUICKSTART.md docs/GPU.md docs/WORKSPACES.md docs/SECURITY.md
    docs/LARGE-PROJECTS.md docs/MULTI-MACHINE.md docs/CONFIGURATION.md docs/MONITORING.md
    examples/settings.json examples/runspoke-monitor.png
    examples/runspk.service examples/runspk-scheduler.service examples/runspk-worker.service
    examples/runspk-joined-worker.service examples/runspk-tunnel.service
    examples/single.env examples/scheduler.env examples/worker.env
    examples/python-project/experiment.py examples/python-project/measurements.csv
    examples/python-project/runspk.json examples/python-project/.runspkignore
)
for package_file in "${package_files[@]}"; do
    install -Dm 644 "$package_file" "$package_stage/$package_file"
done
# Record the highest glibc symbol required by any companion, not the build host's version.
package_glibc=$(
    for binary in runspk runspk-scheduler runspk-worker runspk-bench; do
        readelf --version-info "$package_stage/$binary" || exit 1
    done | sed -n 's/.*Name: GLIBC_\([0-9][0-9.]*\) .*/\1/p' | sort -Vu | tail -n 1
)
[[ "$package_glibc" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || {
    printf 'Cannot determine required glibc version\n' >&2; exit 1;
}
printf '%s x86_64-unknown-linux-gnu %s\n' "$package_version" "$package_glibc" > "$package_stage/RELEASE"
package_members=(runspk runspk-scheduler runspk-worker runspk-bench install.sh RELEASE "${package_files[@]}")
(
    cd "$package_stage"
    sha256sum "${package_members[@]}" > SHA256SUMS
)
package_name="runspoke-$package_version-linux-x86_64.tar.gz"
package_epoch=${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct)}
tar --sort=name --mtime="@$package_epoch" --owner=0 --group=0 --numeric-owner -C "$package_stage" -cf - \
    "${package_members[@]}" SHA256SUMS | gzip -n > "$package_stage/$package_name"
scripts/install.sh "$package_stage/$package_name" "$package_stage/verify-bin"
(
    cd "$package_stage"
    sha256sum "$package_name" > "$package_name.sha256"
)
mkdir -p "$package_out"
mv -- "$package_stage/$package_name" "$package_stage/$package_name.sha256" "$package_out/"
printf 'Package: %s/%s\n' "$package_out" "$package_name"
