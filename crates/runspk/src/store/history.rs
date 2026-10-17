//! Indexed inspection and bounded history deletion. No active attempt is prunable.
use super::*;
use rusqlite::types::Value;

#[derive(Debug)]
pub struct CompletionPruned;
impl std::fmt::Display for CompletionPruned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("completed job history was pruned")
    }
}
impl std::error::Error for CompletionPruned {}

impl Store {
    pub fn list_filtered(&self, filter: &JobFilter) -> Result<Vec<Job>> {
        ensure!(filter.after >= 0, "after must be a nonnegative sequence");
        ensure!(
            filter.limit.is_none_or(|n| (1..=1000).contains(&n)),
            "limit must be in 1..=1000"
        );
        let mut sql = String::from("SELECT data FROM jobs WHERE seq>?");
        let mut values = vec![Value::Integer(filter.after)];
        if let Some(state) = filter.state {
            sql.push_str(" AND state=?");
            values.push(Value::Text(
                serde_json::to_value(state)?
                    .as_str()
                    .context("invalid state")?
                    .into(),
            ));
        }
        if let Some(worker) = &filter.worker {
            sql.push_str(" AND worker_id=?");
            values.push(Value::Text(worker.clone()));
        }
        sql.push_str(" ORDER BY seq LIMIT ?");
        values.push(Value::Integer(filter.limit.map_or(-1, i64::from)));
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values), |r| {
            r.get::<_, String>(0)
        })?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }

    pub fn prune(&mut self, request: &PruneRequest) -> Result<usize> {
        ensure!(
            request.before_ms > 0 && request.before_ms <= now_ms(),
            "before_ms must be a past Unix millisecond timestamp"
        );
        ensure!(
            (1..=1000).contains(&request.limit),
            "limit must be in 1..=1000"
        );
        let tx = self.conn.transaction()?;
        let ids = {
            let mut stmt = tx.prepare("SELECT id FROM jobs
                WHERE state IN ('succeeded','failed','cancelled') AND finished_ms < ?1
                AND NOT EXISTS (SELECT 1 FROM submissions WHERE job_id=jobs.id)
                AND NOT EXISTS (SELECT 1 FROM executions WHERE job_id=jobs.id AND json_extract(data,'$.state') IS NOT 'finished')
                ORDER BY finished_ms,seq LIMIT ?2")?;
            stmt.query_map(params![request.before_ms, request.limit], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        if !request.dry_run {
            for id in &ids {
                let workspace: Option<String> =
                    tx.query_row("SELECT workspace_id FROM jobs WHERE id=?1", [id], |r| {
                        r.get(0)
                    })?;
                tx.execute("DELETE FROM artifacts WHERE job_id=?1", [id])?;
                tx.execute("DELETE FROM attempts WHERE job_id=?1", [id])?;
                // Completion receipts outlive history so lost acknowledgements remain safe.
                tx.execute("DELETE FROM jobs WHERE id=?1", [id])?;
                if let Some(workspace) = workspace {
                    tx.execute("DELETE FROM workspaces WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM jobs WHERE workspace_id=?1)",[workspace])?;
                }
            }
        }
        tx.commit()?;
        Ok(ids.len())
    }

    /// This is a feasibility explanation, not a promise about a future policy decision.
    pub fn explain(&self, id: &str) -> Result<serde_json::Value> {
        let job = self.get(id)?;
        let active = self.reservations()?;
        let workers: Vec<_> = self.workers()?.into_iter().map(|worker| {
            let free = policy::available(&worker, &active);
            let reason = if worker.state != WorkerState::Online {
                "worker is offline or draining"
            } else if job.spec.workspace_id.is_some() && !worker.registration.project_staging {
                "worker needs an upgrade for project staging"
            } else if (job.spec.image.is_some() && worker.registration.runtime.is_none())
                || (job.spec.image.is_none() && !worker.registration.allow_native)
            {
                "execution backend is disabled"
            } else if !job.spec.resources.fits(worker.registration.capacity) {
                "request exceeds worker capacity"
            } else if policy::gpu_selection(&worker, &job, &[]).is_none() {
                "GPU inventory, memory or affinity cannot fit"
            } else if !job.spec.resources.fits(free) {
                "CPU or RAM is reserved by active or uncertain work"
            } else if policy::gpu_selection(&worker, &job, &active).is_none() {
                "GPUs are reserved by active or uncertain work"
            } else {
                "fits now; dispatch depends on queue order and policy"
            };
            serde_json::json!({"worker_id":worker.registration.auth.id,"state":worker.state,"available":free,"reason":reason})
        }).collect();
        Ok(
            serde_json::json!({"job_id":job.id,"state":job.state,"policy":self.policy,"retry_at_ms":job.retry_at_ms,"retry_delayed":job.retry_at_ms>now_ms(),"workers":workers}),
        )
    }
}
