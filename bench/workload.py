#!/usr/bin/env python3
"""Trusted, bounded CPU/memory workload; prints measurements from inside the process."""
import argparse
import json
import os
import resource
import time

p = argparse.ArgumentParser()
p.add_argument('--cpu-ms', type=int, default=20)
p.add_argument('--mem-mib', type=int, default=8)
p.add_argument('--sleep-ms', type=int, default=0)
a = p.parse_args()
assert 0 <= a.cpu_ms <= 1000 and 0 <= a.mem_mib <= 32 and 0 <= a.sleep_ms <= 5000
started_ms = time.time_ns() // 1_000_000
memory = bytearray(a.mem_mib * 1024 * 1024)
for offset in range(0, len(memory), 4096):
    memory[offset] = 1
until = time.process_time() + a.cpu_ms / 1000
value = 1
while time.process_time() < until:
    for _ in range(1000):
        value = (value * 31 + 17) % 1_000_003
time.sleep(a.sleep_ms / 1000)
usage = resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps({'started_ms': started_ms, 'finished_ms': time.time_ns() // 1_000_000,
                  'cpu_s': usage.ru_utime + usage.ru_stime, 'max_rss_mib': usage.ru_maxrss / 1024,
                  'affinity_cpus': len(os.sched_getaffinity(0)), 'checksum': value}))
