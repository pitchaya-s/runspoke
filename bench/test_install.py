"""Exercise release packaging and installation in private temporary directories."""
import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[1]
INSTALL = REPO / 'scripts/install.sh'
BINARIES = ['runspk', 'runspk-scheduler', 'runspk-worker', 'runspk-bench']
UNLISTED_FILES = [
    'CONTRIBUTING.md', 'docs/unlisted-guide.md',
    'not-in-archive/notes.txt', 'not-in-archive/nested/notes.txt',
]


def binary_script(name, version='0.2.0'):
    return f'#!/bin/sh\n[ "$1" = --version ] || exit 2\nprintf "%s\\n" "{name} {version}"\n'.encode()


def wrapper(root, command, body):
    directory = root / 'commands'
    directory.mkdir(exist_ok=True)
    script = directory / command
    script.write_text('#!/bin/sh\n' + body + '\n')
    script.chmod(0o755)
    return {'PATH': f'{directory}:{os.environ["PATH"]}'}


class InstallerTest(unittest.TestCase):
    def archive(self, root, *, version='0.2.0', files=None, extra=None, manifest=None, corrupt=None):
        archive = root / 'release.tar.gz'
        contents = {name: binary_script(name, version) for name in BINARIES}
        contents['RELEASE'] = f'{version} x86_64-unknown-linux-gnu 2.35\n'.encode()
        contents.update(files or {})
        contents = {name: data for name, data in contents.items() if data is not None}
        checksums = ''.join(
            f'{hashlib.sha256(data).hexdigest()}  {name}\n'
            for name, data in contents.items()).encode()
        contents['SHA256SUMS'] = manifest(checksums) if manifest else checksums
        if corrupt is not None:
            contents[corrupt] += b'corrupted'
        with tarfile.open(archive, 'w:gz') as tar:
            for name, data in contents.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                entry.mode = 0o755 if name in BINARIES else 0o644
                tar.addfile(entry, io.BytesIO(data))
            if extra is not None:
                tar.addfile(extra)
        return archive

    def run_install(self, archive, target, env=None):
        return subprocess.run([INSTALL, archive, target], capture_output=True,
                              env={**os.environ, **(env or {})}, timeout=20)

    def assert_rejected(self, root, archive, env=None):
        target = root / 'bin'
        target.mkdir(exist_ok=True)
        for binary in BINARIES:
            (target / binary).write_bytes(b'old installation')
        result = self.run_install(archive, target, env)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        for binary in BINARIES:
            self.assertEqual((target / binary).read_bytes(), b'old installation')
        self.assertEqual(sorted(p.name for p in target.iterdir()), sorted(BINARIES))
        return result

    def test_fresh_install_and_upgrade_preserve_existing_inode_and_unrelated_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / 'bin with spaces'
            result = self.run_install(self.archive(root, version='0.1.0'), target)
            self.assertEqual(result.returncode, 0, result.stderr)
            (target / 'unrelated').write_text('keep')
            with (target / 'runspk').open('rb') as running:
                result = self.run_install(self.archive(root), target)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(running.read(), binary_script('runspk', '0.1.0'))
            for binary in BINARIES:
                self.assertEqual((target / binary).stat().st_mode & 0o777, 0o755)
                self.assertEqual((target / binary).read_bytes(), binary_script(binary))
            self.assertEqual((target / 'unrelated').read_text(), 'keep')
            self.assertFalse((target / '.runspk-install.lock').exists())

    def test_archive_paths_duplicates_and_special_entries_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            entries = [tarfile.TarInfo(name) for name in (
                '../escape', str(root / 'escape'), '.env', 'worker-connection.json',
                'docs/../escape', 'docs/private.md', 'examples/python-project/.env',
                'runspk', 'SHA256SUMS', 'RELEASE')]
            for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE, tarfile.DIRTYPE):
                entry = tarfile.TarInfo('LICENSE')
                entry.type = kind
                entry.linkname = '/etc/passwd'
                entries.append(entry)
            for entry in entries:
                with self.subTest(name=entry.name, kind=entry.type):
                    self.assert_rejected(root, self.archive(root, extra=entry))
            self.assertFalse((root / 'escape').exists())

    def test_manifest_is_complete_unique_and_cannot_reference_external_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mutations = [
                lambda data: b'',
                lambda data: data.split(b'\n', 1)[1],
                lambda data: data + data.splitlines(keepends=True)[0],
                lambda data: data.replace(b'  runspk\n', b'  ../runspk\n'),
                lambda data: data.replace(b'  runspk\n', b'  /etc/passwd\n'),
                lambda data: data.replace(b'  runspk\n', b'  absent\n'),
                lambda data: b'invalid checksum\n' + data,
            ]
            for mutation in mutations:
                with self.subTest(mutation=mutation):
                    self.assert_rejected(root, self.archive(root, manifest=mutation))
            for missing in [*BINARIES, 'RELEASE']:
                with self.subTest(missing=missing):
                    self.assert_rejected(root, self.archive(root, files={missing: None}))
            for corrupt in ['runspk', 'RELEASE', 'docs/QUICKSTART.md']:
                with self.subTest(corrupt=corrupt):
                    self.assert_rejected(root, self.archive(
                        root, files={'docs/QUICKSTART.md': b'reviewed docs'}, corrupt=corrupt))
            truncated = root / 'truncated.tar.gz'
            truncated.write_bytes(b'not gzip')
            self.assert_rejected(root, truncated)

    def test_archives_with_unlisted_files_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in UNLISTED_FILES:
                with self.subTest(name=name):
                    result = self.assert_rejected(
                        root, self.archive(root, files={name: b'not an allowed release member'}))
                    self.assertIn(b'Unexpected archive entry:', result.stderr)

    def test_platform_glibc_release_metadata_and_companion_versions(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for release in (b'bad\n', b'0.2.0 aarch64-unknown-linux-gnu 2.35\n',
                            b'0.2.0 x86_64-unknown-linux-gnu 99.0\n',
                            b'0.2.0 x86_64-unknown-linux-gnu bad\n',
                            b'0.2.0 x86_64-unknown-linux-gnu 2.35\nextra\n'):
                with self.subTest(release=release):
                    self.assert_rejected(root, self.archive(root, files={'RELEASE': release}))
            for binary in BINARIES:
                with self.subTest(binary=binary):
                    self.assert_rejected(root, self.archive(root, files={binary: binary_script(binary, '0.1.0')}))
            self.assert_rejected(root, self.archive(root, files={'runspk-worker': binary_script('runspk-scheduler')}))
            self.assert_rejected(root, self.archive(root, files={'runspk-scheduler': binary_script('runspk-worker')}))
            self.assert_rejected(root, self.archive(root, files={'runspk-worker': b'#!/bin/sh\nexit 1\n'}))
            for command, body in (
                ('uname', 'echo Darwin'),
                ('uname', 'if [ "$1" = -s ]; then echo Linux; else echo aarch64; fi'),
                ('getconf', 'echo "musl 1.2"'),
                ('getconf', 'echo "glibc 2.9"'),
                ('getconf', 'exit 1'),
            ):
                with self.subTest(command=command, body=body):
                    env = wrapper(root, command, body)
                    self.assert_rejected(root, self.archive(root), env)
                    (root / 'commands' / command).unlink()
            result = self.run_install(self.archive(root, version='0.2.0-beta.1'), root / 'beta')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assert_rejected(root, self.archive(root, files={'runspk-bench': binary_script('runspk')}))

    def test_version_probe_kills_a_binary_that_ignores_term(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            hanging = f'#!{sys.executable}\nimport signal, time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(60)\n'.encode()
            result = self.assert_rejected(root, self.archive(root, files={'runspk-worker': hanging}))
            self.assertIn(b'Cannot check runspk-worker --version', result.stderr)

    def test_destination_links_special_files_and_locks_are_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / 'bin'
            target.mkdir()
            outside = root / 'outside'
            outside.write_text('keep')
            archive = self.archive(root)
            for kind in ('symlink', 'directory', 'fifo'):
                path = target / 'runspk-worker'
                if kind == 'symlink':
                    path.symlink_to(outside)
                elif kind == 'directory':
                    path.mkdir()
                else:
                    os.mkfifo(path)
                result = self.run_install(archive, target)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outside.read_text(), 'keep')
                self.assertEqual(list(target.iterdir()), [path])
                path.rmdir() if kind == 'directory' else path.unlink()
            lock = target / '.runspk-install.lock'
            lock.mkdir()
            (lock / 'saved').write_text('recovery data')
            result = self.run_install(archive, target)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual((lock / 'saved').read_text(), 'recovery data')

    def test_copy_rename_and_signal_failures_rollback_upgrades(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = self.archive(root)
            for command, failure in (
                ('install', 'exit 1'),
                ('mv', 'exit 1'),
                ('mv', 'kill -TERM "$PPID"; exit 1'),
            ):
                with self.subTest(command=command, failure=failure):
                    original = shutil.which(command)
                    env = wrapper(root, command,
                                  f'case "$*" in */new/runspk-worker*) {failure};; esac\nexec {original} "$@"')
                    self.assert_rejected(root, archive, env)
                    (root / 'commands' / command).unlink()
            env = wrapper(root, 'mv',
                          f'case "$*" in */new/runspk-worker*) exit 1;; esac\nexec {shutil.which("mv")} "$@"')
            fresh = root / 'fresh'
            result = self.run_install(archive, fresh, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(fresh.iterdir()), [])

    def test_rollback_failure_preserves_backups_for_manual_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / 'bin'
            target.mkdir()
            for binary in BINARIES:
                (target / binary).write_bytes(b'old installation')
            env = wrapper(root, 'mv',
                          f'case "$*" in */new/runspk-worker*|*/old/runspk-scheduler*) exit 1;; esac\nexec {shutil.which("mv")} "$@"')
            result = self.run_install(self.archive(root), target, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'Rollback incomplete', result.stderr)
            backup = target / '.runspk-install.lock/old/runspk-scheduler'
            self.assertEqual(backup.read_bytes(), b'old installation')


class PackageTest(unittest.TestCase):
    def fixture(self, root):
        for directory in ['scripts', 'docs', 'examples']:
            (root / directory).mkdir()
        for script in ['package.sh', 'install.sh', 'verify-release.sh']:
            shutil.copy2(REPO / 'scripts' / script, root / 'scripts' / script)
        for directory in ['docs', 'examples']:
            shutil.copytree(REPO / directory, root / directory, dirs_exist_ok=True)
        for name in ['LICENSE', 'README.md', 'CHANGELOG.md']:
            shutil.copy2(REPO / name, root / name)
        for name in UNLISTED_FILES:
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('Not included in the release member allowlist.\n')
        binaries = root / 'target/x86_64-unknown-linux-gnu/release'
        binaries.mkdir(parents=True)
        for binary in BINARIES:
            path = binaries / binary
            path.write_bytes(binary_script(binary))
            path.chmod(0o755)
        wrapper(root, 'cargo', 'printf "%s\\n" "$@" > cargo-args')
        env = wrapper(root, 'readelf', 'echo "Name: GLIBC_2.34 Flags: none"\necho "Name: GLIBC_2.35 Flags: none"')
        return {**os.environ, **env, 'SOURCE_DATE_EPOCH': '1700000000'}

    def package(self, root, env):
        return subprocess.run([root / 'scripts/package.sh'], cwd=root, env=env,
                              capture_output=True, timeout=30)

    def test_versioned_reproducible_archive_keeps_product_docs_and_excludes_unlisted_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = self.fixture(root)
            result = self.package(root, env)
            self.assertEqual(result.returncode, 0, result.stderr)
            archive = root / 'dist/runspoke-0.2.0-linux-x86_64.tar.gz'
            first = archive.read_bytes()
            sidecar = archive.with_suffix('.gz.sha256').read_text()
            self.assertEqual(sidecar, f'{hashlib.sha256(first).hexdigest()}  {archive.name}\n')
            with tarfile.open(archive) as tar:
                names = tar.getnames()
                self.assertEqual(len(names), len(set(names)))
                self.assertTrue(all(member.isfile() for member in tar))
                for name in ['README.md', 'CHANGELOG.md', 'docs/QUICKSTART.md', 'docs/LARGE-PROJECTS.md', 'docs/MULTI-MACHINE.md',
                             'docs/CONFIGURATION.md', 'docs/MONITORING.md', 'examples/settings.json',
                             'examples/runspoke-monitor.png',
                             'examples/runspk-joined-worker.service', 'examples/runspk-tunnel.service',
                             'examples/python-project/.runspkignore']:
                    self.assertIn(name, names)
                self.assertFalse(set(names).intersection(UNLISTED_FILES))
                self.assertEqual(tar.extractfile('RELEASE').read(), b'0.2.0 x86_64-unknown-linux-gnu 2.35\n')
                manifest = tar.extractfile('SHA256SUMS').read().decode().splitlines()
                self.assertEqual({line.split('  ')[1] for line in manifest}, set(names) - {'SHA256SUMS'})
            args = (root / 'cargo-args').read_text().splitlines()
            self.assertIn('--locked', args)
            self.assertEqual(args[args.index('--target') + 1], 'x86_64-unknown-linux-gnu')
            result = self.package(root, env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(archive.read_bytes(), first)

    def test_unsupported_platform_mixed_versions_and_missing_docs_do_not_emit_archives(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = self.fixture(root)
            wrapper(root, 'uname', 'echo aarch64')
            result = self.package(root, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / 'cargo-args').exists())
            (root / 'commands/uname').unlink()
            binary = root / 'target/x86_64-unknown-linux-gnu/release/runspk-worker'
            binary.write_bytes(binary_script('runspk-worker', '0.1.0'))
            result = self.package(root, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'Companion version mismatch', result.stderr)
            self.assertFalse((root / 'dist').exists())
            binary.write_bytes(binary_script('runspk-worker'))
            (root / 'docs/LARGE-PROJECTS.md').unlink()
            result = self.package(root, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / 'dist').exists())

    def test_release_verifier_checks_sidecar_install_and_product_test_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = self.fixture(root)
            binary = root / 'target/x86_64-unknown-linux-gnu/release/runspk'
            binary.write_bytes(binary_script('runspk').replace(
                b'[ "$1" = --version ] || exit 2',
                b'if [ "$1" = completions ]; then echo completion; exit 0; fi\n[ "$1" = --version ] || exit 2'))
            result = self.package(root, env)
            self.assertEqual(result.returncode, 0, result.stderr)
            wrapper(root, 'cargo',
                    'test -x "$RUNSPK_PRODUCT_BIN_DIR/runspk" || exit 1\n'
                    'test -f "$RUNSPK_PRODUCT_EXAMPLE_DIR/runspk.json" || exit 1\n'
                    'printf "%s\\n" "$@" > cargo-args')
            archive = root / 'dist/runspoke-0.2.0-linux-x86_64.tar.gz'
            verifier = root / 'scripts/verify-release.sh'
            result = subprocess.run([verifier, archive], cwd=root, env=env, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((root / 'cargo-args').read_text().splitlines(),
                             ['test', '--locked', '--test', 'product', '--', '--nocapture'])
            archive.write_bytes(archive.read_bytes() + b'corrupted')
            result = subprocess.run([verifier, archive], cwd=root, env=env, capture_output=True, timeout=30)
            self.assertNotEqual(result.returncode, 0)
