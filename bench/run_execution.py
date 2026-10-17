#!/usr/bin/env python3
"""Run identical fixed-seed trusted workloads through real scheduler/worker processes.

At most two CPU slots and 384 MiB of declared worker RAM; no physical GPU use.
Every case has a 25 second bound. All processes, files and signals are scoped to this run.
"""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
POLICIES = ['fifo', 'best-fit', 'priority', 'easy']
SCENARIOS = ['short', 'long', 'mixed', 'priority', 'heterogeneous', 'failure', 'fragmentation', 'gpu', 'bursty']
ACTIVE = {'assigned', 'running', 'cancelling', 'uncertain'}
TERMINAL = {'succeeded', 'failed', 'cancelled'}
TOKEN_ENV = 'RUNSPK_TOKEN'


def public_result(value):
    """Keep local checkout names out of publishable records; preserve measured hashes."""
    if isinstance(value, str):
        return value.replace(str(ROOT), '$PROJECT_ROOT')
    if isinstance(value, list):
        return [public_result(item) for item in value]
    if isinstance(value, dict):
        return {key: public_result(item) for key, item in value.items()}
    return value


def worker_cpu_ids():
    cores = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
        key = (topology.joinpath('physical_package_id').read_text().strip(),
               topology.joinpath('core_id').read_text().strip())
        cores.setdefault(key, cpu)
    assert len(cores) >= 2, 'benchmark requires two available physical cores'
    return list(cores.values())[:2]


def workload(scenario, seed):
    rng = random.Random(seed)
    count = {'short': 24, 'long': 4, 'mixed': 16, 'priority': 20, 'heterogeneous': 16,
             'failure': 8, 'fragmentation': 12, 'gpu': 8, 'bursty': 24}[scenario]
    result = []
    for i in range(count):
        cpu_ms = 400 if scenario == 'long' else rng.choice([10, 40, 80])
        sleep_ms = 1500 if scenario == 'failure' else (40 if scenario == 'long' else 10)
        mem_mib = rng.choice([4, 8, 16])
        request_mem = 256 if scenario in {'heterogeneous', 'fragmentation'} and i % 3 == 1 else 64
        arrival = 0.3 if scenario == 'priority' and i >= 16 else (i // 8 * 0.4 if scenario == 'bursty' else 0)
        spec = {'command': [sys.executable, str(ROOT / 'bench/workload.py'), '--cpu-ms', str(cpu_ms),
                            '--mem-mib', str(mem_mib), '--sleep-ms', str(sleep_ms)],
                'resources': {'cpu': 1, 'mem_mib': request_mem}, 'timeout_secs': 12,
                'trusted': True, 'cwd': None, 'priority': 50 if scenario == 'priority' and i >= 16 else 0,
                'max_retries': 1 if scenario == 'failure' else 0, 'submission_key': f'{scenario}-{seed}-{i}',
                'image': None, 'gpu': 1 if scenario == 'gpu' else 0, 'gpu_ids': [], 'gpu_mem_mib': 0}
        result.append({'index': i, 'arrival_s': arrival, 'spec': spec})
    result.sort(key=lambda j: (j['arrival_s'], j['index']))
    return result


def api(url, token, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url + path, data=data,
                                     headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=2) as response:
        return json.load(response)


def read_proc(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
        return {'cpu': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
                'rss': int(fields[21]) * os.sysconf('SC_PAGE_SIZE') / 1024**2,
                'start': int(fields[19]), 'state': fields[0]}
    except (FileNotFoundError, ProcessLookupError):
        return None


def descendants(roots):
    pending, visited = list(roots), set()
    while pending:
        pid = pending.pop()
        if pid in visited:
            continue
        visited.add(pid)
        for path in Path(f'/proc/{pid}/task').glob('*/children'):
            try:
                pending.extend(map(int, path.read_text().split()))
            except (FileNotFoundError, ProcessLookupError):
                pass
    return visited


def journals(temp):
    result = []
    for path in Path(temp).glob('*/*.json'):
        try:
            result.append(json.loads(path.read_text()))
        except FileNotFoundError:
            pass
    return result


def quantile(values, fraction):
    return sorted(values)[math.ceil((len(values) - 1) * fraction)]


def run_case(bin_dir, out, scenario, policy, seed, generated):
    pins = worker_cpu_ids()
    with tempfile.TemporaryDirectory(prefix='runspk-bench-') as temp, (out / f'{scenario}-{policy}.log').open('w') as log:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            address = f'127.0.0.1:{sock.getsockname()[1]}'
        url, token = 'http://' + address, secrets.token_hex(32)
        env = dict({k: v for k, v in os.environ.items() if not k.startswith('RUNSPK_')}, RUNSPK_TOKEN=token, RUNSPK_URL=url, RUST_LOG='warn')
        scheduler = subprocess.Popen([str(bin_dir / 'runspk-scheduler'), '--bind', address, '--db',
                                      str(Path(temp) / 'state.sqlite'), '--policy', policy], env=env,
                                     stdout=subprocess.DEVNULL, stderr=log)
        workers = []
        known_groups = {}
        agents = {scheduler.pid}
        cpu_by_pid = {}
        max_agent_rss = 0
        try:
            for _ in range(100):
                try:
                    api(url, token, '/jobs')
                    break
                except (OSError, urllib.error.URLError):
                    assert scheduler.poll() is None, 'scheduler exited'
                    time.sleep(0.03)
            else:
                raise AssertionError('scheduler startup timed out')

            def spawn_worker(index):
                command = ['taskset', '--cpu-list', str(pins[index]), str(bin_dir / 'runspk-worker'), '--id', f'worker-{index}', '--cpu', '1',
                           '--mem-mib', str(256 if index == 0 else 128), '--allow-native', '--data-dir', temp]
                if scenario == 'gpu':
                    command += ['--simulate-gpus', '1']
                worker = subprocess.Popen(command, env=env, stdout=subprocess.DEVNULL, stderr=log)
                agents.add(worker.pid)
                return worker

            workers = [spawn_worker(0), spawn_worker(1)]
            for _ in range(100):
                if len(api(url, token, '/workers')) == 2:
                    break
                assert all(w.poll() is None for w in workers), 'worker exited'
                time.sleep(0.03)
            else:
                raise AssertionError('worker startup timed out')
            started = previous = time.perf_counter()
            deadline = started + 25
            index = 0
            submitted = []
            crash_ms = restart_at = None
            crashed_job = None
            restart_done = False
            jobs = []
            cpu_area = mem_area = stranded_area = rss_area = 0.0
            samples = 0
            while True:
                now = time.perf_counter()
                assert now < deadline, f'{scenario}/{policy}: benchmark deadline exceeded'
                elapsed = now - started
                while index < len(generated) and generated[index]['arrival_s'] <= elapsed:
                    job = api(url, token, '/jobs', generated[index]['spec'])
                    submitted.append(job['id'])
                    index += 1
                jobs = api(url, token, '/jobs')
                inventory = api(url, token, '/workers')
                active = [j for j in jobs if j['state'] in ACTIVE]
                for w in inventory:
                    assert w['reserved']['cpu'] <= w['registration']['capacity']['cpu']
                    assert w['reserved']['mem_mib'] <= w['registration']['capacity']['mem_mib']
                gpu_ids = [g for j in active for g in j['allocated_gpus']]
                assert len(gpu_ids) == len(set(gpu_ids)), 'GPU overcommit'
                dt = now - previous
                previous = now
                cpu_area += sum(j['spec']['resources']['cpu'] for j in active) * dt
                mem_area += sum(j['spec']['resources']['mem_mib'] for j in active) * dt
                ready = [j for j in jobs if j['state'] == 'queued' and j['retry_at_ms'] <= time.time_ns() // 1_000_000]
                for w in inventory:
                    free_cpu = w['registration']['capacity']['cpu'] - w['reserved']['cpu']
                    free_mem = w['registration']['capacity']['mem_mib'] - w['reserved']['mem_mib']
                    used = {g for j in active if j['worker_id'] == w['registration']['auth']['id'] for g in j['allocated_gpus']}
                    free_gpu = len([g for g in w['registration']['gpus'] if g['id'] not in used])
                    if ready and w['state'] == 'online' and not any(j['spec']['resources']['cpu'] <= free_cpu and j['spec']['resources']['mem_mib'] <= free_mem and j['spec']['gpu'] <= free_gpu for j in ready):
                        stranded_area += free_cpu * dt
                entries = journals(temp)
                for entry in entries:
                    p = entry.get('process')
                    if p:
                        known_groups[p['pid']] = p
                roots = [w.pid for w in workers if w.poll() is None] + list(known_groups)
                all_pids = descendants(roots) | agents
                job_rss = agent_rss = 0
                for pid in all_pids:
                    measured = read_proc(pid)
                    if measured:
                        if pid in agents:
                            cpu_by_pid[pid] = max(cpu_by_pid.get(pid, 0), measured['cpu'])
                            agent_rss += measured['rss']
                        else:
                            job_rss += measured['rss']
                rss_area += job_rss * dt
                max_agent_rss = max(max_agent_rss, agent_rss)
                samples += 1
                if scenario == 'failure' and crash_ms is None and elapsed >= 0.25:
                    candidates = [j for j in jobs if j['worker_id'] == 'worker-0' and j['state'] == 'running']
                    if candidates and any(e.get('process') and e['assignment']['job']['id'] == candidates[0]['id'] for e in entries):
                        crashed_job = candidates[0]['id']
                        crash_ms = time.time_ns() // 1_000_000
                        workers[0].kill()
                        workers[0].wait()
                if crash_ms and not restart_done:
                    uncertain = next(j for j in jobs if j['id'] == crashed_job)['state'] == 'uncertain'
                    if uncertain and restart_at is None:
                        restart_at = now + 0.1
                    if restart_at and now >= restart_at:
                        workers[0] = spawn_worker(0)
                        restart_done = True
                if index == len(generated) and len(jobs) == len(generated) and all(j['state'] in TERMINAL for j in jobs):
                    break
                time.sleep(0.02)
            makespan = time.perf_counter() - started
            assert all(j['state'] == 'succeeded' for j in jobs), f'failed jobs: {jobs}'
            stats = api(url, token, '/stats')
            waits, dispatch, fairness, traces, payload_cpu = [], [], [], [], 0.0
            recovery = None
            for j in jobs:
                attempts = api(url, token, f"/jobs/{j['id']}/attempts")
                started_attempts = [a for a in attempts if a['started_ms'] is not None]
                first_start = min(a['started_ms'] for a in started_attempts)
                wait = (first_start - j['submitted_ms']) / 1000
                waits.append(wait)
                dispatch.extend((a['started_ms'] - a['assigned_ms']) for a in started_attempts)
                logs = api(url, token, f"/jobs/{j['id']}/logs")
                payload = json.loads(logs['stdout'])
                assert payload['affinity_cpus'] <= 1
                payload_cpu += payload['cpu_s']
                service = max(0.01, (payload['finished_ms'] - payload['started_ms']) / 1000)
                fairness.append(1 / max(1, (j['finished_ms'] - j['submitted_ms']) / 1000 / service))
                if j['id'] == crashed_job:
                    assert len(started_attempts) == 2, 'one stopped execution should retry exactly once'
                    recovery = max(a['started_ms'] for a in started_attempts) - crash_ms
                traces.append({'scenario': scenario, 'policy': policy, 'seed': seed, 'job': j,
                               'attempts': attempts, 'payload_measurements': payload})
            if scenario == 'failure':
                assert crash_ms is not None and restart_done and recovery is not None
            for pid in agents:
                measured = read_proc(pid)
                if measured:
                    cpu_by_pid[pid] = max(cpu_by_pid.get(pid, 0), measured['cpu'])
            timing = stats['scheduling']
            row = {'scenario': scenario, 'policy': policy, 'seed': seed, 'jobs': len(jobs), 'worker_cpu_ids': ','.join(map(str, pins)),
                   'workload_sha256': hashlib.sha256(json.dumps(generated, sort_keys=True).encode()).hexdigest(),
                   'makespan_s': makespan, 'throughput_jobs_s': len(jobs) / makespan,
                   'mean_wait_s': sum(waits) / len(waits), 'p95_wait_s': quantile(waits, 0.95), 'max_wait_s': max(waits),
                   'mean_dispatch_ms': sum(dispatch) / len(dispatch), 'reserved_cpu_fraction': cpu_area / (2 * makespan),
                   'reserved_mem_fraction': mem_area / (384 * makespan), 'stranded_cpu_fraction': stranded_area / (2 * makespan),
                   'measured_payload_cpu_fraction': payload_cpu / (2 * makespan),
                   'sampled_job_rss_fraction': rss_area / (384 * makespan), 'peak_agent_rss_mib': max_agent_rss,
                   'scheduler_cpu_s': cpu_by_pid.get(scheduler.pid, 0),
                   'worker_cpu_s': sum(value for pid, value in cpu_by_pid.items() if pid != scheduler.pid),
                   'jain_inverse_slowdown': sum(fairness)**2 / (len(fairness) * sum(f*f for f in fairness)),
                   'policy_mean_us': timing['policy_total_ns'] / max(1, timing['passes']) / 1000,
                   'policy_max_us': timing['policy_max_ns'] / 1000, 'failure_recovery_ms': recovery,
                   'reexecutions': sum(j['attempts'] - 1 for j in jobs), 'samples': samples}
            return row, traces
        finally:
            for worker in workers:
                if worker.poll() is None:
                    worker.send_signal(signal.SIGTERM)
                    try:
                        worker.wait(timeout=6)
                    except subprocess.TimeoutExpired:
                        worker.kill()
                        worker.wait()
            if scheduler.poll() is None:
                scheduler.send_signal(signal.SIGTERM)
                try:
                    scheduler.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    scheduler.kill()
                    scheduler.wait()
            for p in known_groups.values():
                measured = read_proc(p['pid'])
                if measured is None or measured['start'] == p['start_ticks']:
                    try:
                        os.killpg(p['pid'], signal.SIGKILL)
                    except ProcessLookupError:
                        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bin-dir', type=Path, default=ROOT / 'target/release')
    parser.add_argument('--out', type=Path, default=ROOT / 'bench/results/execution')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--scenarios', nargs='+', choices=SCENARIOS, default=SCENARIOS)
    parser.add_argument('--policies', nargs='+', choices=POLICIES, default=POLICIES)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    assert all((args.bin_dir / name).is_file() for name in ['runspk', 'runspk-worker', 'runspk-scheduler'])
    generated = {s: workload(s, args.seed) for s in args.scenarios}
    assert generated == {s: workload(s, args.seed) for s in args.scenarios}
    (args.out / 'workloads.json').write_text(json.dumps(public_result(generated), indent=2) + '\n')
    summaries, traces = [], []
    for scenario in args.scenarios:
        for policy in args.policies:
            row, trace = run_case(args.bin_dir.resolve(), args.out, scenario, policy, args.seed, generated[scenario])
            summaries.append(row)
            traces.extend(trace)
            (args.out / 'execution.json').write_text(json.dumps(summaries, indent=2) + '\n')
            (args.out / 'execution-traces.json').write_text(json.dumps(public_result(traces)) + '\n')
            with (args.out / 'execution.csv').open('w') as file:
                writer = csv.DictWriter(file, fieldnames=list(row))
                writer.writeheader()
                writer.writerows(summaries)
            print(f"{scenario:14} {policy:8} {row['jobs']:3} jobs {row['makespan_s']:.3f}s "
                  f"{row['throughput_jobs_s']:.2f} jobs/s recovery={row['failure_recovery_ms']}", flush=True)
    for scenario in args.scenarios:
        assert len({r['workload_sha256'] for r in summaries if r['scenario'] == scenario}) == 1


if __name__ == '__main__':
    main()
