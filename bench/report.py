#!/usr/bin/env python3
"""Capture reproducibility metadata and render measured benchmark tables/plots."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import platform
import statistics as st
import subprocess

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'bench/results'
POLICIES = ['fifo', 'best-fit', 'priority', 'easy']
COLORS = ['#0072B2', '#E69F00', '#009E73', '#CC79A7']
SCENARIOS = ['short', 'long', 'mixed', 'priority', 'heterogeneous', 'failure', 'fragmentation', 'gpu', 'bursty']


def public_hardware(value):
    for index, gpu in enumerate(value.get('gpus', [])):
        gpu['id'] = f'GPU-REDACTED-{index}'
    return value


def capture():
    import os
    files = sorted([*ROOT.glob('crates/**/*.rs'), ROOT / 'Cargo.toml', ROOT / 'Cargo.lock',
                    ROOT / 'bench/workload.py', ROOT / 'bench/run_execution.py'])
    digest = hashlib.sha256()
    for file in files:
        digest.update(str(file.relative_to(ROOT)).encode() + b'\0' + file.read_bytes())
    env = dict(os.environ)
    if (ROOT / '.tools/cargo/bin/rustc').exists():
        env.update(RUSTUP_HOME=str(ROOT / '.tools/rustup'), CARGO_HOME=str(ROOT / '.tools/cargo'))
        compiler = ROOT / '.tools/cargo/bin/rustc'
    else:
        compiler = 'rustc'
    from run_execution import worker_cpu_ids
    metadata = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'platform': platform.platform(),
                'python': platform.python_version(), 'compiler': subprocess.check_output([str(compiler), '--version'], env=env, text=True).strip(),
                'source_sha256': digest.hexdigest(), 'source_files': [str(p.relative_to(ROOT)) for p in files],
                'base_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                'simulation_seeds': [42, 7, 1337], 'execution_seed': 42,
                'execution_trials_per_policy_scenario': 1,
                'execution_budget': {'cpu_slots': 2, 'worker_memory_mib': 384, 'physical_gpu_jobs': 0, 'worker_cpu_ids': worker_cpu_ids()}}
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / 'host.json').write_text(json.dumps(metadata, indent=2) + '\n')
    hardware = subprocess.check_output([str(ROOT / 'target/release/runspk'), 'hardware'], text=True)
    (RESULTS / 'hardware.json').write_text(json.dumps(public_hardware(json.loads(hardware)), indent=2) + '\n')


def grouped_plot(plt, rows, key, title, ylabel, path, normalize=False):
    fig, ax = plt.subplots(figsize=(13, 5), layout='constrained')
    for index, policy in enumerate(POLICIES):
        means, lows, highs = [], [], []
        for scenario in SCENARIOS:
            selected = [r for r in rows if r['scenario'] == scenario and r['policy'] == policy]
            values = []
            for row in selected:
                baseline = next(r for r in rows if r['scenario'] == scenario and r['policy'] == 'fifo' and r['seed'] == row['seed'])
                values.append(row[key] / baseline[key] if normalize else row[key])
            average = st.mean(values)
            means.append(average)
            lows.append(average - min(values))
            highs.append(max(values) - average)
        ax.bar([i + (index - 1.5) * 0.2 for i in range(len(SCENARIOS))], means, 0.19,
               label=policy, color=COLORS[index], yerr=[lows, highs], capsize=2)
    ax.set_xticks(range(len(SCENARIOS)), SCENARIOS, rotation=22, ha='right')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_ylim(bottom=0)
    ax.grid(axis='y', alpha=0.25)
    ax.set_axisbelow(True)
    ax.legend(ncol=4)
    if normalize:
        ax.axhline(1, color='#555555', linewidth=0.8, linestyle='--')
    fig.savefig(path.with_suffix('.svg'))
    fig.savefig(path.with_suffix('.png'), dpi=160)
    plt.close(fig)


def report():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    simulations = []
    for path in sorted(RESULTS.glob('seed-*/simulation.json')):
        simulations.extend(json.loads(path.read_text()))
    actual = json.loads((RESULTS / 'execution/execution.json').read_text())
    traces = json.loads((RESULTS / 'execution/execution-traces.json').read_text())
    assert len(actual) == 36 and len(simulations) == 108
    for scenario in SCENARIOS:
        assert len({r['workload_sha256'] for r in actual if r['scenario'] == scenario}) == 1
    plots = RESULTS / 'plots'
    plots.mkdir(exist_ok=True)
    grouped_plot(plt, simulations, 'makespan_s', 'Simulation: makespan relative to FIFO (3 seeds; whiskers show range)',
                 'Makespan / FIFO makespan (lower is shorter)', plots / 'simulation-makespan', normalize=True)
    grouped_plot(plt, actual, 'makespan_s', 'Real execution: one bounded trial per scenario and policy',
                 'Elapsed seconds', plots / 'execution-makespan')
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), layout='constrained')
    for ax, columns, title in [(axes[0], [('reserved_cpu_fraction', 'reserved', COLORS[0]), ('measured_payload_cpu_fraction', 'payload CPU', COLORS[1])], 'CPU: declarations and process CPU time'),
                               (axes[1], [('reserved_mem_fraction', 'reserved', COLORS[0]), ('sampled_job_rss_fraction', 'sampled RSS', COLORS[2])], 'RAM: declarations and sampled RSS')]:
        for offset, (key, label, color) in enumerate(columns):
            values = [100 * st.mean(r[key] for r in actual if r['scenario'] == s) for s in SCENARIOS]
            ax.bar([i + (offset - 0.5) * 0.35 for i in range(len(SCENARIOS))], values, 0.34, label=label, color=color)
        ax.set_xticks(range(len(SCENARIOS)), SCENARIOS, rotation=45, ha='right')
        ax.set_ylim(0, 105)
        ax.set_ylabel('Percent of configured cluster budget')
        ax.set_title(title)
        ax.legend()
        ax.grid(axis='y', alpha=0.25)
        ax.set_axisbelow(True)
    fig.suptitle('Real execution: mean across four policy runs; failure CPU omits the lost attempt')
    fig.savefig(plots / 'reservations-and-usage.svg')
    fig.savefig(plots / 'reservations-and-usage.png', dpi=160)
    plt.close(fig)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout='constrained')
    for ax, rows, title in [(axes[0], simulations, 'Simulation: up to 120 jobs'), (axes[1], actual, 'Live execution: up to 24 jobs')]:
        values = [st.mean(r['policy_mean_us'] for r in rows if r['policy'] == p) for p in POLICIES]
        ax.bar(POLICIES, values, color=COLORS)
        ax.set_ylabel('Mean policy call (microseconds)')
        ax.set_title(title)
        ax.grid(axis='y', alpha=0.25)
        ax.set_axisbelow(True)
    fig.savefig(plots / 'policy-overhead.svg')
    fig.savefig(plots / 'policy-overhead.png', dpi=160)
    plt.close(fig)
    short = [r for r in actual if r['scenario'] == 'short']
    lines = ['# Measured benchmark results', '',
             'These are local measurements on the host in [host.json](host.json) and [hardware.json](hardware.json). '
             'Execution used two worker processes on distinct physical cores, two CPU slots and 384 MiB of declared RAM. Physical GPU kernels were not run. '
             'The GPU scenario uses simulated exclusive identities and CPU payloads.', '',
             'Source fingerprints and any post-measurement review changes are recorded in host.json. '
             'The measured fingerprint is preserved when later safety or documentation edits are made.', '',
             '## Real execution', '',
             'Each cell is one elapsed-time measurement in seconds. Inputs are identical across policies within each scenario '
             '(verified by SHA-256 in execution.csv). One trial is not a statistical performance claim; interpreter startup, '
             'fsyncs, the 100 ms worker poll, log polling, benchmark sampling and other host activity affect results.', '',
             '| Scenario | FIFO | Best fit | Priority | EASY |', '| --- | ---: | ---: | ---: | ---: |']
    for scenario in SCENARIOS:
        values = [next(r for r in actual if r['scenario'] == scenario and r['policy'] == p)['makespan_s'] for p in POLICIES]
        lines.append(f"| {scenario} | " + ' | '.join(f'{v:.3f}' for v in values) + ' |')
    lines += ['', f"Short-job throughput ranged from {min(r['throughput_jobs_s'] for r in short):.2f} to {max(r['throughput_jobs_s'] for r in short):.2f} jobs/s for 24 Python payloads. "
              f"Peak combined scheduler/worker RSS ranged from {min(r['peak_agent_rss_mib'] for r in actual):.1f} to {max(r['peak_agent_rss_mib'] for r in actual):.1f} MiB. "
              f"Scheduler CPU per completed job ranged from {min(r['scheduler_cpu_s']/r['jobs']*1000 for r in actual):.2f} to {max(r['scheduler_cpu_s']/r['jobs']*1000 for r in actual):.2f} ms.", '',
              '| Policy | High-priority mean queue wait (s) | Failure to retry start (ms) |', '| --- | ---: | ---: |']
    for policy in POLICIES:
        high = [t for t in traces if t['scenario'] == 'priority' and t['policy'] == policy and t['job']['spec']['priority'] > 0]
        waits = [(min(a['started_ms'] for a in t['attempts'] if a['started_ms'] is not None) - t['job']['submitted_ms']) / 1000 for t in high]
        recovery = next(r for r in actual if r['scenario'] == 'failure' and r['policy'] == policy)['failure_recovery_ms']
        lines.append(f'| {policy} | {st.mean(waits):.3f} | {recovery} |')
    lines += ['', 'Failure runs kill only their own worker, wait for its job to become uncertain, restart it from the same journal, '
              'and measure until the stopped attempt is retried. The interval includes the five-second lease and one-second retry backoff. '
              'A permanently unreachable host would remain uncertain until fenced; this benchmark does not imply automatic safe failover.', '',
              '## Simulated policy comparisons', '',
              'Nine scenarios use 120 jobs (12 for long jobs), with seeds 42, 7 and 1337. Every policy receives the same generated workload '
              'and worker capacities for each seed. Values below are mean makespan divided by FIFO for that seed; whiskers in the plot show '
              'the three-seed range, not confidence intervals. Runtime, arrivals and failure/fencing events advance a virtual clock. '
              'Policy call timings measure real Rust CPU work; makespan and reservation utilization are simulated.', '',
              '| Scenario | FIFO | Best fit | Priority | EASY |', '| --- | ---: | ---: | ---: | ---: |']
    for scenario in SCENARIOS:
        ratios = []
        for policy in POLICIES:
            selected = [r for r in simulations if r['scenario'] == scenario and r['policy'] == policy]
            ratios.append(st.mean(r['makespan_s'] / next(b['makespan_s'] for b in simulations if b['scenario'] == scenario and b['policy'] == 'fifo' and b['seed'] == r['seed']) for r in selected))
        lines.append(f'| {scenario} | ' + ' | '.join(f'{v:.3f}' for v in ratios) + ' |')
    lines += ['', 'FIFO is predictable but can leave capacity idle behind a blocked head. Best fit can preserve a worker needed by a larger '
              'request, but its CPU/memory score does not optimize the full future queue. Priority changes which jobs wait; aging bounds '
              'priority advantage under the documented capacity/runtimes assumptions. EASY can fill gaps on other workers or before its '
              'head reservation; conservative timeout slack can also leave gaps unused. The tables show workload-dependent outcomes, '
              'including regressions and cases where policies tie. No universal policy improvement is claimed.', '',
              '## Metric definitions and limits', '',
              '- Queue wait is first control-plane start acknowledgement minus submission; dispatch latency is start acknowledgement minus placement. '
              'Trace payload timestamps allow separate inspection of actual interpreter startup.',
              '- Reservation utilization integrates active declared CPU/RAM over wall time and divides by the configured cluster budget. '
              'It includes dispatch, cancellation and uncertainty. It is not hardware usage.',
              '- Payload CPU comes from each successful Python process\'s getrusage. In failure runs the lost attempt is unavailable, so this is a lower bound. '
              'RSS samples cover owned workload process trees at roughly 20 ms intervals; brief processes and peaks can be missed. '
              'Payload max RSS is also retained in traces. Agent CPU/RSS is sampled separately from job descendants.',
              '- Fragmentation is free CPU stranded on a worker where no ready job fits CPU, RAM and GPUs, integrated over time and normalized by total CPU capacity. '
              'It does not count every queue-order-induced idle interval.',
              '- Fairness is Jain\'s index of inverse bounded slowdown (turnaround / service time, clamped to at least one). '
              'It reflects this workload, not multi-user or long-term fair-share guarantees.',
              '- Policy timings exclude SQLite, HTTP, fsync, dispatch, launch, monitoring and cleanup. Scheduler CPU per job and elapsed makespan include those costs. '
              'Placement and heartbeat accounting read indexed live jobs. Full list/stat responses and the active queue remain serialized under one mutex and are not paginated, which limits large queues and histories.',
              '- Simulated failure includes a two-second fencing acknowledgement before retry; actual failure uses journal-backed process cleanup. '
              'Neither models arbitrary storage corruption or kernel stalls. No real multi-host network or physical GPU throughput was benchmarked.', '',
              '## Refinement evidence', '', '[Diagnostic baselines](baseline/README.md) preserve the initial measurements. The isolated absent-process-group cleanup probe took 70,355 microseconds for 20 calls before the ESRCH fast path and 160 microseconds after, in one release-build batch each. This narrow measurement does not establish an end-to-end speedup. Final benchmarks use disjoint physical-core affinity; earlier diagnostic runs did not.', '', '## Artifacts', '',
              '[Execution CSV](execution/execution.csv) · [execution JSON](execution/execution.json) · '
              '[execution traces](execution/execution-traces.json) · [seed 42 simulation CSV](seed-42/simulation.csv) · '
              '[seed 7 CSV](seed-7/simulation.csv) · [seed 1337 CSV](seed-1337/simulation.csv)', '',
              '![Simulation makespan](plots/simulation-makespan.png)', '', '![Real execution makespan](plots/execution-makespan.png)', '',
              '![Reservations and observed usage](plots/reservations-and-usage.png)', '', '![Policy timing](plots/policy-overhead.png)', '']
    (RESULTS / 'REPORT.md').write_text('\n'.join(lines))
    print(f'{len(simulations)} simulated comparisons, {len(actual)} real comparisons; report and four SVG/PNG plots written')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--capture', action='store_true')
    args = parser.parse_args()
    capture() if args.capture else report()
