"""Disposable SSH transport check; no installed services or SSH files are edited."""
import json
import os
from pathlib import Path
import pwd
import signal
import socket
import subprocess
import tempfile
import time

os.umask(0o077)
repo = Path(__file__).resolve().parents[1]
binary = Path(os.environ.get('RUNSPK_PRODUCT_BIN_DIR', repo / 'target/debug')) / 'runspk'
children = []
env = {k: v for k, v in os.environ.items() if not k.startswith('RUNSPK_')}
env['PATH'] = '/usr/bin:/bin'

def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

def run(args, **kwargs):
    return subprocess.run(args, env=env, capture_output=True, timeout=15, **kwargs)

def start(args, log, **kwargs):
    child = subprocess.Popen(args, env=env, stdin=subprocess.DEVNULL,
                             stdout=log, stderr=log, **kwargs)
    children.append(child)
    return child

with tempfile.TemporaryDirectory(prefix='.runspk-ssh-test-', dir=Path.home()) as temp:
    root = Path(temp)
    log = (root / 'processes.log').open('w+')
    server_port, ssh_port, tunnel_port = port(), port(), port()
    config = root / 'scheduler.json'
    def cli(*args, forwarded=False):
        url = f'http://127.0.0.1:{tunnel_port if forwarded else server_port}'
        return run([binary, '--config', config, '--url', url, *args])
    try:
        for key in ['host', 'client']:
            result = run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', root / key])
            assert result.returncode == 0, result.stderr.decode()
        host_key = ' '.join((root / 'host.pub').read_text().split()[:2])
        (root / 'known_hosts').write_text(f'[127.0.0.1]:{ssh_port} {host_key}\n')
        username = pwd.getpwuid(os.getuid()).pw_name
        (root / 'sshd_config').write_text(f'''Port {ssh_port}
ListenAddress 127.0.0.1
HostKey {root}/host
PidFile {root}/sshd.pid
AuthorizedKeysFile {root}/client.pub
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
StrictModes yes
AllowUsers {username}
AllowTcpForwarding local
PermitOpen 127.0.0.1:{server_port}
LogLevel ERROR
''')
        sshd = start(['/usr/sbin/sshd', '-D', '-e', '-f', root / 'sshd_config'], log)
        result = cli('init', '--state-dir', str(root / 'state'))
        assert result.returncode == 0, result.stderr.decode()
        scheduler = start([binary, '--config', config, 'start', '--scheduler-only'], log)
        deadline = time.monotonic() + 10
        while cli('list').returncode:
            assert time.monotonic() < deadline, 'scheduler startup failed'
            time.sleep(.02)
        tunnel_args = ['ssh', '-F', '/dev/null', '-NT', '-o', 'BatchMode=yes',
                       '-o', 'ExitOnForwardFailure=yes', '-o', 'StrictHostKeyChecking=yes',
                       '-o', f'UserKnownHostsFile={root}/known_hosts', '-o', 'IdentitiesOnly=yes',
                       '-i', root / 'client', '-p', str(ssh_port),
                       '-L', f'127.0.0.1:{tunnel_port}:127.0.0.1:{server_port}',
                       f'{username}@127.0.0.1']
        tunnel = start(tunnel_args, log)
        deadline = time.monotonic() + 10
        while cli('list', forwarded=True).returncode:
            assert sshd.poll() is None and tunnel.poll() is None, 'SSH tunnel exited'
            assert time.monotonic() < deadline, 'SSH tunnel startup failed'
            time.sleep(.02)
        invitation = root / 'invitation.json'
        profile = json.loads(config.read_text())
        profile['url'] = f'http://127.0.0.1:{tunnel_port}'
        profile['state_dir'] = None
        invitation.write_text(json.dumps(profile))
        env['XDG_STATE_HOME'] = str(root / 'worker-state')
        worker = start([binary, '--config', root / 'worker.json', 'join', invitation,
                        '--id', 'ssh-worker', '--cpu', '1', '--mem', '128M', '--allow-native'], log)
        deadline = time.monotonic() + 10
        while not json.loads(cli('workers', forwarded=True).stdout):
            assert worker.poll() is None, 'worker exited'
            assert time.monotonic() < deadline, 'worker did not join through SSH'
            time.sleep(.02)
        result = cli('run', '--mem', '64M', '--trusted', '--', '/bin/echo', 'ssh-ok', forwarded=True)
        assert result.returncode == 0 and result.stdout == b'ssh-ok\n', result.stderr.decode()
        result = cli('submit', '--mem', '64M', '--timeout', '30', '--trusted', '--', '/bin/sleep', '20')
        assert result.returncode == 0
        job_id = result.stdout.decode().strip()
        deadline = time.monotonic() + 10
        while json.loads(cli('status', job_id).stdout)['state'] != 'running':
            assert time.monotonic() < deadline
            time.sleep(.02)
        tunnel.terminate()
        tunnel.wait(timeout=5)
        assert cli('list', forwarded=True).returncode != 0
        deadline = time.monotonic() + 10
        journals = root / 'worker-state/runspk/workers/ssh-worker'
        while True:
            pending = [json.loads(p.read_text()) for p in journals.glob('*.json')]
            if pending and pending[0].get('completion'):
                assert pending[0]['completion']['error'] == 'execution lease could not be renewed'
                break
            assert time.monotonic() < deadline, 'lost SSH transport did not stop execution durably'
            time.sleep(.02)
        tunnel = start(tunnel_args, log)
        result = cli('wait', job_id, '--timeout', '10')
        assert result.returncode == 137, (result.returncode, result.stderr.decode())
        result = cli('run', '--mem', '64M', '--trusted', '--', '/bin/echo', 'reconnected')
        assert result.returncode == 0 and result.stdout == b'reconnected\n'
        print('PASS: real SSH strict host-key tunnel, worker execution, disconnection fencing, durable completion replay and reconnection')
    except Exception:
        log.flush()
        log.seek(0)
        print(log.read())
        raise
    finally:
        for child in reversed(children):
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)
                try:
                    child.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
