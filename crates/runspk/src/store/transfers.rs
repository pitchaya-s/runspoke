//! Snapshot persistence is transactional with submission; artifacts are immutable per attempt.
use super::*;
use crate::workspace::{ArtifactUpload, Artifacts, Bundle};

impl Store {
    pub fn workspace(&self, id: &str) -> Result<Bundle> {
        let data: Option<Vec<u8>> = self
            .conn
            .query_row("SELECT data FROM workspaces WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        let bundle: Bundle = serde_json::from_slice(&data.context("workspace not found")?)?;
        ensure!(bundle.id()? == id, "stored workspace fingerprint mismatch");
        Ok(bundle)
    }
    pub fn save_artifacts(&mut self, upload: ArtifactUpload) -> Result<()> {
        upload.bundle.validate()?;
        let attempt = self.owned(&upload.owner)?;
        ensure!(
            attempt.started_ms.is_some(),
            "outputs require a start acknowledgement"
        );
        ensure!(
            attempt.state != AttemptState::Assigned,
            "outputs require a started or finished attempt"
        );
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=?1)",
            [&attempt.job_id],
            |r| r.get(0),
        )?;
        if !exists && attempt.state == AttemptState::Finished {
            return Err(CompletionPruned.into());
        }
        let job = self.get(&attempt.job_id)?;
        ensure!(job.spec.workspace_id.is_some(), "job has no staged project");
        ensure!(
            upload
                .bundle
                .files
                .iter()
                .all(
                    |file| job.spec.outputs.iter().any(|output| file.path == *output
                        || file
                            .path
                            .strip_prefix(output)
                            .is_some_and(|p| p.starts_with('/')))
                ),
            "output was not declared by this job"
        );
        let data = serde_json::to_vec(&upload.bundle)?;
        let old: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT data FROM artifacts WHERE attempt_id=?1",
                [&attempt.id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            ensure!(old == data, "attempt outputs are immutable");
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO artifacts(attempt_id,job_id,data) VALUES(?1,?2,?3)",
            params![attempt.id, attempt.job_id, data],
        )?;
        Ok(())
    }
    pub fn artifacts(&self, id: &str, attempt: Option<&str>) -> Result<Artifacts> {
        let job = self.get(id)?;
        let attempt = attempt
            .or(job.attempt_id.as_deref())
            .context("job has no attempt yet; use runspk wait first")?;
        let execution = self.attempt(attempt)?;
        ensure!(execution.job_id == id, "attempt belongs to another job");
        ensure!(
            execution.state == AttemptState::Finished,
            "outputs are available after stopped completion; use runspk wait first"
        );
        let data: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT data FROM artifacts WHERE attempt_id=?1",
                [attempt],
                |r| r.get(0),
            )
            .optional()?;
        Ok(Artifacts {
            attempt_id: attempt.into(),
            bundle: serde_json::from_slice(&data.context(
                "outputs not found; declare --output with --project and inspect job errors",
            )?)?,
        })
    }
}
