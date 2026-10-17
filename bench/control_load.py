#!/usr/bin/env python3
"""Count actual idle worker requests through a loopback-only measurement proxy.

No workloads, containers or GPUs run. The proxy adds overhead equally to both builds.
"""
import argparse
from collections import defaultdict
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time

from run_execution import api, read_proc, quantile


class Observer:
    def __init__(self, upstream):
        self.samples = defaultdict(list)
        self.lock = threading.Lock()
        observer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_POST(self):
                started = time.perf_counter_ns()
                body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                connection = http.client.HTTPConnection(upstream, timeout=3)
                try:
                    connection.request('POST', self.path, body, {
                        'Authorization': self.headers.get('Authorization', ''),
                        'Content-Type': 'application/json'})
                    response = connection.getresponse()
                    result = response.read()
                    elapsed = time.perf_counter_ns() - started
                    with observer.lock:
                        observer.samples[self.path].append(elapsed)
                    self.send_response(response.status)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(result)))
                    self.end_headers()
                    self.wfile.write(result)
                finally:
                    connection.close()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def reset(self):
        with self.lock:
            self.samples.clear()

    def summary(self):
        with self.lock:
            return {path: {'count': len(values), 'median_ms': quantile(values, .5) / 1e6,
                           'p95_ms': quantile(values, .95) / 1e6}
                    for path, values in self.samples.items()}

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def measure(bin_dir, seconds):
    with tempfile.TemporaryDirectory(prefix='runspk-idle-') as temp:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            address = f'127.0.0.1:{sock.getsockname()[1]}'
        token = secrets.token_hex(32)
        env = {k: v for k, v in os.environ.items() if not k.startswith('RUNSPK_')}
        env.update(RUNSPK_TOKEN=token, RUST_LOG='warn')
        scheduler = subprocess.Popen([str(bin_dir / 'runspk-scheduler'), '--bind', address,
                                      '--db', str(Path(temp) / 'state.sqlite')], env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        worker = observer = None
        try:
            for _ in range(100):
                try:
                    api('http://' + address, token, '/stats')
                    break
                except OSError:
                    assert scheduler.poll() is None, 'scheduler exited'
                    time.sleep(.03)
            else:
                raise AssertionError('scheduler startup timed out')
            observer = Observer(address)
            env['RUNSPK_URL'] = observer.url
            worker = subprocess.Popen([str(bin_dir / 'runspk-worker'), '--id', 'idle', '--cpu',
                                       '1', '--mem-mib', '64', '--allow-native', '--data-dir', temp],
                                      env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(100):
                if api('http://' + address, token, '/workers'):
                    break
                assert worker.poll() is None, 'worker exited'
                time.sleep(.03)
            else:
                raise AssertionError('worker startup timed out')
            observer.reset()
            cpu_start = read_proc(scheduler.pid)['cpu']
            started = time.perf_counter()
            time.sleep(seconds)
            elapsed = time.perf_counter() - started
            observed = read_proc(scheduler.pid)
            return {'elapsed_s': elapsed, 'scheduler_cpu_s': observed['cpu'] - cpu_start,
                    'scheduler_rss_mib': observed['rss'], 'requests': observer.summary()}
        finally:
            for child in (worker, scheduler):
                if child is not None and child.poll() is None:
                    child.send_signal(signal.SIGTERM)
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
            if observer is not None:
                observer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    rows = [measure(args.bin_dir.resolve(), 5) for _ in range(3)]
    args.out.write_text(json.dumps(rows, indent=2) + '\n')


if __name__ == '__main__':
    main()
