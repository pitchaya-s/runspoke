use crate::model::*;
use anyhow::{Context, Result, ensure};
pub fn cpu_ids() -> Result<Vec<usize>> {
    // SAFETY: cpu_set_t is a bitmap with a valid all-zero representation.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: set is initialized, writable and has the exact buffer size passed to libc.
    ensure!(
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) }
            == 0,
        "cannot read CPU affinity"
    );
    Ok((0..libc::CPU_SETSIZE as usize)
        // SAFETY: every index is below CPU_SETSIZE and set remains initialized.
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .collect())
}
pub fn discover_gpus() -> Result<Vec<Gpu>> {
    let output = match std::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=uuid,name,memory.total,memory.free,utilization.gpu",
            "--format=csv,noheader,nounits",
        ])
        .output()
    {
        Ok(out) => out,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        output.status.success(),
        "nvidia-smi failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    parse_gpus(&String::from_utf8(output.stdout)?)
}
pub fn parse_gpus(text: &str) -> Result<Vec<Gpu>> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let fields: Vec<_> = line.split(',').map(str::trim).collect();
            ensure!(fields.len() == 5, "unexpected nvidia-smi CSV");
            ensure!(valid_gpu_id(fields[0]), "invalid GPU identity");
            Ok(Gpu {
                id: fields[0].into(),
                name: fields[1].into(),
                memory_mib: fields[2].parse().context("GPU memory")?,
                free_memory_mib: fields[3].parse().context("GPU free memory")?,
                utilization_percent: fields[4].parse().context("GPU utilization")?,
                simulated: false,
            })
        })
        .collect()
}
pub fn select_gpus(ids: &[String], simulate: u32, worker: &str) -> Result<Vec<Gpu>> {
    ensure!(
        ids.is_empty() || simulate == 0,
        "choose physical or simulated GPUs"
    );
    ensure!(simulate <= 64, "maximum 64 simulated GPUs");
    if simulate > 0 {
        return Ok((0..simulate)
            .map(|i| Gpu {
                id: format!("sim:{worker}:{i}"),
                name: "simulated".into(),
                memory_mib: 4096,
                free_memory_mib: 4096,
                utilization_percent: 0,
                simulated: true,
            })
            .collect());
    }
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let found = discover_gpus()?;
    ids.iter()
        .map(|id| {
            found
                .iter()
                .find(|g| &g.id == id)
                .cloned()
                .with_context(|| format!("GPU {id} not discovered"))
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_discovery_and_simulates_without_hardware() {
        let g = parse_gpus("GPU-123, RTX 3060, 6144, 5800, 3\n").unwrap();
        assert_eq!(g[0].memory_mib, 6144);
        assert_eq!(g[0].utilization_percent, 3);
        assert!(parse_gpus("broken").is_err());
        assert_eq!(select_gpus(&[], 2, "test").unwrap().len(), 2);
    }
}
