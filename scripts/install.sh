#!/usr/bin/env bash
# Install a reviewed local release archive. Never edits services, PATH or user data.
set -euo pipefail
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf 'Usage: %s ARCHIVE [BIN_DIRECTORY]\n' "$0" >&2
    exit 2
fi
fail() { printf '%s\n' "$*" >&2; exit 1; }
[ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] || fail 'Installation requires Linux x86_64'
install_host_glibc=$(getconf GNU_LIBC_VERSION) || fail 'Installation requires glibc'
[[ "$install_host_glibc" =~ ^glibc\ ([0-9]+\.[0-9]+(\.[0-9]+)?)$ ]] || fail 'Cannot determine host glibc version'
install_host_glibc=${BASH_REMATCH[1]}
install_archive=$(realpath "$1")
install_root=${2:-"$HOME/.local/bin"}
install_stage=$(mktemp -d)
install_lock=
install_changed=()
install_committed=0
cleanup() {
    local status=$? rollback_failed=0 binary
    trap - EXIT HUP INT TERM
    if [ "$install_committed" = 0 ]; then
        for binary in "${install_changed[@]}"; do
            if [ -f "$install_lock/old/$binary" ]; then
                mv -fT -- "$install_lock/old/$binary" "$install_root/$binary" || rollback_failed=1
            else
                rm -f -- "$install_root/$binary" || rollback_failed=1
            fi
        done
    fi
    if [ "$rollback_failed" = 1 ]; then
        printf 'Rollback incomplete; recover original binaries from %s/old before removing the lock\n' "$install_lock" >&2
        status=1
    elif [ -n "$install_lock" ]; then
        rm -rf -- "$install_lock"
    fi
    rm -rf -- "$install_stage"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
tar -tzf "$install_archive" > "$install_stage/members"
while IFS= read -r member; do
    case "$member" in
        runspk|runspk-scheduler|runspk-worker|runspk-bench|install.sh|LICENSE|README.md|CHANGELOG.md|RELEASE|SHA256SUMS) ;;
        docs/ARCHITECTURE.md|docs/FAILURE-MODEL.md|docs/SCHEDULING.md|docs/DEPLOYMENT.md|docs/WORKFLOWS.md|docs/QUICKSTART.md|docs/GPU.md|docs/WORKSPACES.md|docs/SECURITY.md|docs/LARGE-PROJECTS.md|docs/MULTI-MACHINE.md|docs/CONFIGURATION.md|docs/MONITORING.md) ;;
        examples/runspk.service|examples/runspk-scheduler.service|examples/runspk-worker.service|examples/runspk-joined-worker.service|examples/runspk-tunnel.service|examples/settings.json|examples/runspoke-monitor.png|examples/single.env|examples/scheduler.env|examples/worker.env) ;;
        examples/python-project/experiment.py|examples/python-project/measurements.csv|examples/python-project/runspk.json|examples/python-project/.runspkignore) ;;
        QUICKSTART.md|python-project/experiment.py|python-project/measurements.csv|python-project/runspk.json|python-project/.runspkignore) ;;
        *) fail "Unexpected archive entry: $member" ;;
    esac
done < "$install_stage/members"
awk 'seen[$0]++ {exit 1}' "$install_stage/members" || fail 'Duplicate archive entry'
tar -tvzf "$install_archive" > "$install_stage/types"
awk 'substr($0,1,1)!="-" {exit 1}' "$install_stage/types" || fail 'Archive links and special entries are forbidden'
mkdir "$install_stage/files"
tar -xzf "$install_archive" --no-same-owner --no-same-permissions -C "$install_stage/files"
for member in runspk runspk-scheduler runspk-worker runspk-bench RELEASE SHA256SUMS; do
    [ -f "$install_stage/files/$member" ] || fail "Missing archive entry: $member"
done
# Bind every checksum to one allowed archive member before sha256sum reads any path.
awk '
    NR==FNR {members[$0]=1; next}
    NF!=2 || length($1)!=64 || $1 ~ /[^0-9a-f]/ || !($2 in members) || $2=="SHA256SUMS" || seen[$2]++ {bad=1}
    END {for (name in members) if (name!="SHA256SUMS" && !seen[name]) bad=1; exit bad}
' "$install_stage/members" "$install_stage/files/SHA256SUMS" || fail 'Invalid, missing or duplicate manifest checksum'
(
    cd "$install_stage/files"
    sha256sum --check --strict SHA256SUMS
)
read -r install_version install_target install_glibc < "$install_stage/files/RELEASE"
[[ "$install_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.+-]+)?$ ]] || fail 'Invalid release version'
[ "$install_target" = x86_64-unknown-linux-gnu ] || fail "Unsupported release target: $install_target"
[[ "$install_glibc" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || fail 'Invalid release glibc version'
[ "$(cat "$install_stage/files/RELEASE")" = "$install_version $install_target $install_glibc" ] || fail 'Invalid RELEASE metadata'
printf '%s\n' "$install_glibc" "$install_host_glibc" | sort -CV || fail "Release requires glibc $install_glibc; host has $install_host_glibc"
for binary in runspk runspk-scheduler runspk-worker runspk-bench; do
    chmod 755 "$install_stage/files/$binary"
    install_reported=$(timeout --signal=KILL --foreground 5 "$install_stage/files/$binary" --version) || fail "Cannot check $binary --version"
    [ "$install_reported" = "$binary $install_version" ] || fail "Companion version mismatch: $binary reported $install_reported; expected $install_version"
done
mkdir -p -- "$install_root"
install_root=$(realpath "$install_root")
mkdir -- "$install_root/.runspk-install.lock" || fail 'Another installation or recovery lock exists'
install_lock="$install_root/.runspk-install.lock"
mkdir "$install_lock/new" "$install_lock/old"
for binary in runspk runspk-scheduler runspk-worker runspk-bench; do
    [ ! -L "$install_root/$binary" ] || fail "Refusing destination symlink: $binary"
    if [ -e "$install_root/$binary" ]; then
        [ -f "$install_root/$binary" ] || fail "Destination is not a regular file: $binary"
        cp -p -- "$install_root/$binary" "$install_lock/old/$binary"
    fi
    install -m 755 "$install_stage/files/$binary" "$install_lock/new/$binary"
done
for binary in runspk runspk-scheduler runspk-worker runspk-bench; do
    install_changed+=("$binary")
    mv -fT -- "$install_lock/new/$binary" "$install_root/$binary"
done
install_committed=1
printf 'Installed runspk %s in %s\nEnsure this directory is on PATH. Next: runspk init\n' "$install_version" "$install_root"
