//! SQLite online backup uses a separate read connection, never the scheduler mutex.
use anyhow::{Result, ensure};
use rusqlite::{
    Connection, OpenFlags,
    backup::{Backup, StepResult},
};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    time::{Duration, Instant},
};

pub fn backup_database(source: &Path, destination: &Path) -> Result<()> {
    let reader = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)?;
    let result = (|| -> Result<()> {
        let mut writer = Connection::open(destination)?;
        let backup = Backup::new(&reader, &mut writer)?;
        let started = Instant::now();
        loop {
            if backup.step(128)? == StepResult::Done {
                break;
            }
            ensure!(
                started.elapsed() < Duration::from_secs(30),
                "backup did not finish within 30 seconds"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(backup);
        writer.close().map_err(|(_, e)| e)?;
        file.sync_all()?;
        File::open(
            destination
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(destination);
    }
    result
}
