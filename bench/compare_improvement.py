#!/usr/bin/env python3
"""Validate matched inputs and summarize the committed before/after experiments."""
import argparse
import json
from pathlib import Path
import statistics
from run_execution import quantile


def describe(values):
    assert values
    return {'median': statistics.median(values), 'min': min(values), 'max': max(values),
            'p95': quantile(values, .95), 'stdev': statistics.pstdev(values)}


def compare(root):
    def read(label, path):
        return json.loads((root / label / path).read_text())

    micro = {}
    suites = {}
    short = {}
    idle = {}
    for label in ('baseline', 'after'):
        micro[label] = read(label, 'control-plane.json')
        suites[label] = read(label, 'execution/execution.json')
        short[label] = [row for row in suites[label] if row['scenario'] == 'short' and row['policy'] == 'fifo']
        for seed in (7, 1337):
            short[label] += read(label, f'repeat-{seed}/execution.json')
        idle[label] = read(label, 'idle.json')

    def case(row):
        return row['scenario'], row['policy'], row['seed']

    original = {case(row): row for row in suites['baseline']}
    improved = {case(row): row for row in suites['after']}
    assert original.keys() == improved.keys()
    for key in original:
        assert original[key]['workload_sha256'] == improved[key]['workload_sha256'], key
        assert original[key]['worker_cpu_ids'] == improved[key]['worker_cpu_ids'], key
    for old, new in zip(short['baseline'], short['after'], strict=True):
        assert old['workload_sha256'] == new['workload_sha256']

    traces = 0
    for seed in (42, 7, 1337):
        old = read('baseline', f'sim-{seed}/simulation-traces.json')
        assert old == read('after', f'sim-{seed}/simulation-traces.json'), seed
        traces += len(old)

    keys = lambda row: (row['repeat'], row['history'], row['queued'], row['workers'], row['policy'])
    assert [keys(row) for row in micro['baseline']['rows']] == [keys(row) for row in micro['after']['rows']]
    cells = []
    for history in (0, 5000):
        for workers in (1, 16):
            for policy in ('fifo', 'best-fit', 'priority', 'easy'):
                cell = {'history': history, 'queued': 1000, 'workers': workers, 'policy': policy}
                for label in ('baseline', 'after'):
                    rows = [row for row in micro[label]['rows'] if row['history'] == history
                            and row['workers'] == workers and row['policy'] == policy]
                    cell[label] = {op: describe([row[op]['median_us'] for row in rows])
                                   for op in ('poll', 'heartbeat', 'stats', 'expire')}
                    for op in ('poll', 'heartbeat', 'stats', 'expire'):
                        cell[label][op]['pooled_p95_us'] = quantile(
                            [sample / 1000 for row in rows for sample in row[op]['samples_ns']], .95)
                cells.append(cell)

    execution = {}
    for label in ('baseline', 'after'):
        direct_traces = read(label, 'execution/execution-traces.json')
        dispatch = [attempt['started_ms'] - attempt['assigned_ms'] for trace in direct_traces
                    for attempt in trace['attempts'] if attempt['started_ms'] is not None]
        assert len(direct_traces) == sum(row['jobs'] for row in suites[label])
        execution[label] = {
            'full_suite_jobs': sum(row['jobs'] for row in suites[label]),
            'full_suite_job_time_s': sum(row['makespan_s'] for row in suites[label]),
            'scheduler_cpu_s': sum(row['scheduler_cpu_s'] for row in suites[label]),
            'peak_combined_agent_rss_mib': max(row['peak_agent_rss_mib'] for row in suites[label]),
            'dispatch_ms': describe(dispatch),
            'recovery_ms': describe([row['failure_recovery_ms'] for row in suites[label] if row['failure_recovery_ms'] is not None]),
            'short_fifo_jobs_s': describe([row['throughput_jobs_s'] for row in short[label]]),
            'short_fifo_seeds': [{'seed': row['seed'], 'jobs_s': row['throughput_jobs_s'], 'makespan_s': row['makespan_s']} for row in short[label]],
            'idle_requests_s': describe([sum(v['count'] for v in row['requests'].values()) / row['elapsed_s'] for row in idle[label]]),
            'idle_scheduler_cpu_s': describe([row['scheduler_cpu_s'] for row in idle[label]]),
            'idle_scheduler_rss_mib': describe([row['scheduler_rss_mib'] for row in idle[label]]),
        }
    return {'matched_execution_cases': len(original), 'identical_simulation_traces': traces,
            'microseconds_trial_medians': cells, 'execution': execution,
            'note': 'Microbenchmarks use in-memory SQLite. Short FIFO variance spans seeds, not identical-input replicate trials. Idle CPU has 10ms sampling granularity; proxy timings include forwarding overhead.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent / 'results/improvement')
    args = parser.parse_args()
    print(json.dumps(compare(args.root), indent=2))
