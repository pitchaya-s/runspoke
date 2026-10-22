//! Isolated fingerprint cost: each mode runs in its own process for peak-RSS measurement.
use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use runspk::workspace::{Bundle, SnapshotFile};
use sha2::{Digest, Sha256};
fn legacy_id(bundle: &Bundle) -> Result<String> {
    let mut bytes = 0;
    for file in &bundle.files {
        runspk::workspace::validate_path(&file.path)?;
        bytes += STANDARD.decode(&file.data)?.len();
        ensure!(
            bytes <= runspk::workspace::MAX_BYTES,
            "legacy bundle exceeds 32 MiB"
        );
    }
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(bundle)?)))
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(args.len() == 3, "usage: transfer_cost BYTES bounded|legacy");
    let bytes: usize = args[1].parse()?;
    ensure!(
        bytes > 0 && bytes <= runspk::workspace::MAX_BYTES,
        "use 1B..=32MiB"
    );
    ensure!(
        matches!(args[2].as_str(), "bounded" | "legacy"),
        "unknown mode"
    );
    // Construct encoded input directly so initial raw-file allocation does not mask fingerprint RSS.
    let mut data = "eHh4".repeat(bytes / 3);
    data.push_str(&STANDARD.encode(vec![b'x'; bytes % 3]));
    let bundle = Bundle {
        files: vec![SnapshotFile {
            path: "input.bin".into(),
            data,
            executable: false,
        }],
    };
    let start = std::time::Instant::now();
    let id = if args[2] == "bounded" {
        bundle.id()?
    } else {
        legacy_id(&bundle)?
    };
    println!(
        "{}",
        serde_json::json!({"mode":args[2],"bytes":bytes,"fingerprint_ms":start.elapsed().as_secs_f64()*1000.0,"id":id})
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_and_streamed_hashes_match() {
        let b = Bundle {
            files: vec![SnapshotFile {
                path: "a".into(),
                data: STANDARD.encode(b"hello\0world"),
                executable: true,
            }],
        };
        assert_eq!(legacy_id(&b).unwrap(), b.id().unwrap());
    }
}
