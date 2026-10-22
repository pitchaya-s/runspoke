//! Measured Linux host telemetry for the entire machine, even when worker CPU
//! affinity differs. These are not reservations or per-job/cgroup measurements.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Read, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostTelemetry {
    pub sampled_ms: i64,
    pub cpu_busy_percent: Option<u32>,
    pub mem_total_mib: u64,
    pub mem_available_mib: u64,
}

impl HostTelemetry {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.sampled_ms > 0, "sample timestamp must be positive");
        ensure!(
            self.cpu_busy_percent.is_none_or(|percent| percent <= 100),
            "CPU busy percent must be at most 100"
        );
        ensure!(
            self.mem_available_mib <= self.mem_total_mib,
            "available memory exceeds total memory"
        );
        ensure!(
            self.mem_total_mib <= 16 * 1024 * 1024,
            "total memory exceeds 16777216 MiB"
        );
        Ok(())
    }
}

/// Starts one sampler for this subscription; dropping all receivers stops it.
pub fn subscribe() -> watch::Receiver<Option<HostTelemetry>> {
    let (sender, receiver) = watch::channel(None);
    tokio::spawn(async move {
        let mut previous = None;
        loop {
            if sender.is_closed() {
                return;
            }
            // Await each blocking read before another can start; cancellation exits.
            let read = tokio::task::spawn_blocking(move || sample(previous));
            let result = tokio::select! {
                biased;
                _ = sender.closed() => return,
                result = read => result,
            };
            let telemetry = match result {
                Ok(Ok((telemetry, counters))) => {
                    previous = Some(counters);
                    Some(telemetry)
                }
                _ => {
                    previous = None;
                    None
                }
            };
            if sender.send(telemetry).is_err() {
                return;
            }
            tokio::select! {
                biased;
                _ = sender.closed() => return,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {},
            }
        }
    });
    receiver
}

fn sample(previous: Option<CpuCounters>) -> Result<(HostTelemetry, CpuCounters)> {
    let cpu = parse_cpu(&read_proc("/proc/stat", 512 * 1024)?)?;
    let (mem_total_mib, mem_available_mib) = parse_memory(&read_proc("/proc/meminfo", 64 * 1024)?)?;
    let telemetry = HostTelemetry {
        sampled_ms: crate::model::now_ms(),
        cpu_busy_percent: cpu.busy_percent(previous),
        mem_total_mib,
        mem_available_mib,
    };
    telemetry.validate()?;
    Ok((telemetry, cpu))
}

fn read_proc(path: &str, limit: u64) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open {path}"))?;
    ensure!(file.metadata()?.is_file(), "{path} is not a regular file");
    let mut text = String::new();
    file.take(limit)
        .read_to_string(&mut text)
        .with_context(|| format!("read {path}"))?;
    ensure!((text.len() as u64) < limit, "{path} reached its read limit");
    Ok(text)
}

#[derive(Clone, Copy, Debug)]
struct CpuCounters([u64; 8]);

impl CpuCounters {
    fn busy_percent(self, previous: Option<Self>) -> Option<u32> {
        let previous = previous?;
        let mut total = 0_u128;
        let mut idle = 0_u128;
        for (index, (current, old)) in self.0.into_iter().zip(previous.0).enumerate() {
            let delta = u128::from(current.checked_sub(old)?);
            total += delta;
            if index == 3 || index == 4 {
                idle += delta;
            }
        }
        // Wider arithmetic keeps sums and percent multiplication from overflowing.
        (total != 0).then(|| ((total - idle) * 100 / total) as u32)
    }
}

fn parse_cpu(text: &str) -> Result<CpuCounters> {
    let mut fields = text
        .lines()
        .next()
        .context("missing CPU counters")?
        .split_whitespace();
    ensure!(
        fields.next() == Some("cpu"),
        "missing aggregate CPU counters"
    );
    let mut counters = [0; 8];
    for counter in &mut counters {
        *counter = fields
            .next()
            .context("missing CPU counter")?
            .parse()
            .context("invalid CPU counter")?;
    }
    // Guest counters overlap user/nice, so only the first eight fields count.
    Ok(CpuCounters(counters))
}

fn parse_memory(text: &str) -> Result<(u64, u64)> {
    let mut total = None;
    let mut available = None;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let target = match fields.next() {
            Some("MemTotal:") => &mut total,
            Some("MemAvailable:") => &mut available,
            _ => continue,
        };
        ensure!(target.is_none(), "duplicate memory field");
        let kib: u64 = fields
            .next()
            .context("missing memory value")?
            .parse()
            .context("invalid memory value")?;
        ensure!(
            fields.next() == Some("kB") && fields.next().is_none(),
            "invalid memory unit or trailing data"
        );
        *target = Some(kib);
    }
    let total = total.context("missing MemTotal")?;
    let available = available.context("missing MemAvailable")?;
    ensure!(available <= total, "available memory exceeds total memory");
    ensure!(
        total <= 16 * 1024 * 1024 * 1024,
        "total memory exceeds limit"
    );
    Ok((total / 1024, available / 1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_initial_and_known_deltas_ignore_guest_and_include_iowait() {
        let before = parse_cpu("cpu 100 20 30 400 50 5 6 7 1000 2000\ncpu0 0\n").unwrap();
        let after = parse_cpu("cpu 120 25 40 440 60 10 11 12 5000 6000\n").unwrap();
        assert_eq!(before.busy_percent(None), None);
        assert_eq!(after.busy_percent(Some(before)), Some(50));
        assert_eq!(before.busy_percent(Some(before)), None);
        assert_eq!(before.busy_percent(Some(after)), None);
        assert_eq!(CpuCounters([0; 8]).busy_percent(Some(before)), None);
    }

    #[test]
    fn cpu_detects_individual_regressions_and_handles_large_counters() {
        let before = CpuCounters([10, 0, 0, 100, 0, 0, 0, 0]);
        let after = CpuCounters([9, 0, 0, 1000, 0, 0, 0, 0]);
        assert_eq!(after.busy_percent(Some(before)), None);
        assert_eq!(
            CpuCounters([u64::MAX; 8]).busy_percent(Some(CpuCounters([0; 8]))),
            Some(75)
        );
        assert_eq!(
            CpuCounters([1, 0, 0, 0, 0, 0, 0, 0]).busy_percent(Some(CpuCounters([0; 8]))),
            Some(100)
        );
        assert_eq!(
            CpuCounters([0, 0, 0, 1, 1, 0, 0, 0]).busy_percent(Some(CpuCounters([0; 8]))),
            Some(0)
        );
    }

    #[test]
    fn rejects_malformed_cpu_counters() {
        for text in [
            "",
            "cpu0 1 2 3 4 5 6 7 8",
            "cpu 1 2 3 4 5 6 7",
            "cpu 1 2 3 4 5 6 7 broken",
            "cpu -1 2 3 4 5 6 7 8",
            "cpu 18446744073709551616 2 3 4 5 6 7 8",
        ] {
            assert!(parse_cpu(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn memory_uses_available_and_converts_kib_to_mib() {
        assert_eq!(
            parse_memory("MemAvailable: 3073 kB\nMemFree: 1 kB\nMemTotal: 8193 kB\n").unwrap(),
            (8, 3)
        );
        assert_eq!(
            parse_memory("MemTotal: 0 kB\nMemAvailable: 0 kB").unwrap(),
            (0, 0)
        );
        for text in [
            "",
            "MemTotal: 1024 kB",
            "MemTotal: 1024 kB\nMemAvailable: 1025 kB",
            "MemTotal: 1024 MB\nMemAvailable: 0 kB",
            "MemTotal: 1024 kB trailing\nMemAvailable: 0 kB",
            "MemTotal: invalid kB\nMemAvailable: 0 kB",
            "MemTotal: 1024 kB\nMemTotal: 1024 kB\nMemAvailable: 0 kB",
            "MemTotal: 17179869185 kB\nMemAvailable: 0 kB",
            "MemTotal: 18446744073709551616 kB\nMemAvailable: 0 kB",
        ] {
            assert!(parse_memory(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn validates_wire_values() {
        let mut telemetry = HostTelemetry {
            sampled_ms: 1,
            cpu_busy_percent: None,
            mem_total_mib: 16 * 1024 * 1024,
            mem_available_mib: 0,
        };
        assert!(telemetry.validate().is_ok());
        telemetry.cpu_busy_percent = Some(100);
        assert!(telemetry.validate().is_ok());
        telemetry.cpu_busy_percent = Some(101);
        assert!(telemetry.validate().is_err());
        telemetry.cpu_busy_percent = None;
        telemetry.mem_total_mib += 1;
        assert!(telemetry.validate().is_err());
        telemetry.mem_total_mib = 0;
        telemetry.mem_available_mib = 1;
        assert!(telemetry.validate().is_err());
        telemetry.mem_available_mib = 0;
        telemetry.sampled_ms = 0;
        assert!(telemetry.validate().is_err());
        telemetry.sampled_ms = -1;
        assert!(telemetry.validate().is_err());
    }
}
