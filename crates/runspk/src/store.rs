mod backup;
mod history;
mod schema;
mod transfers;
pub use backup::backup_database;
pub use history::CompletionPruned;
pub use schema::SCHEMA_VERSION;

use crate::{
    model::*,
    policy::{self, Policy, SchedulingPolicy},
    state_lock::StateLock,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::{fs::OpenOptions, path::Path};
use uuid::Uuid;

pub struct Store {
    conn: Connection,
    pub policy: Policy,
    pub metrics: SchedulingMetrics,
    pub database_metrics: DatabaseMetrics,
    // Drop SQLite before releasing ownership of its state.
    _lock: Option<StateLock>,
}
#[derive(Default, serde::Serialize)]
pub struct SchedulingMetrics {
    pub passes: u64,
    pub policy_total_ns: u64,
    pub policy_max_ns: u64,
    pub placements: u64,
    pub starts: u64,
    pub queue_wait_total_ms: u64,
    pub dispatch_total_ms: u64,
    pub dispatch_max_ms: u64,
    pub retries: u64,
}
#[derive(Default, serde::Serialize)]
pub struct DatabaseMetrics {
    pub operations: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    pub mutex_wait_ns: u64,
}
impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let lock = if path != Path::new(":memory:") {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path.with_extension("sqlite.lock"))?;
            Some(StateLock::new(file).context("database already owned by another scheduler")?)
        } else {
            None
        };
        let mut conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        schema::initialize(&mut conn)?;
        let mut s = Self {
            conn,
            policy: Policy::Fifo,
            metrics: SchedulingMetrics::default(),
            database_metrics: DatabaseMetrics::default(),
            _lock: lock,
        };
        // Missing execution records cannot prove that a reserved job stopped.
        for mut j in s.reservations()? {
            if policy::active(&j)
                && j.attempt_id
                    .as_ref()
                    .is_some_and(|id| s.attempt(id).is_err())
            {
                j.state = JobState::Uncertain;
                j.error = Some("missing execution record needs explicit reconciliation".into());
                Self::write(&s.conn, &j)?;
            }
        }
        s.expire(now_ms())?;
        Ok(s)
    }
    fn write(conn: &Connection, j: &Job) -> Result<()> {
        tracing::debug!(job_id=%j.id,attempt_id=?j.attempt_id,worker_id=?j.worker_id,state=?j.state,"writing job state");
        conn.execute(
            "UPDATE jobs SET state=?1,data=?2 WHERE id=?3",
            params![
                serde_json::to_value(j.state)?.as_str().unwrap(),
                serde_json::to_string(j)?,
                j.id
            ],
        )?;
        Ok(())
    }
    fn write_attempt(conn: &Connection, a: &Attempt) -> Result<()> {
        conn.execute("INSERT INTO executions(id,job_id,data) VALUES (?1,?2,?3) ON CONFLICT(id) DO UPDATE SET data=excluded.data",params![a.id,a.job_id,serde_json::to_string(a)?])?;
        Ok(())
    }
    pub fn submit(&mut self, spec: JobSpec) -> Result<Job> {
        self.submit_inner(spec, None)
    }
    pub fn submit_project(
        &mut self,
        mut request: crate::workspace::ProjectSubmission,
    ) -> Result<Job> {
        let id = request.workspace.id()?;
        ensure!(
            request
                .spec
                .workspace_id
                .as_ref()
                .is_none_or(|old| old == &id),
            "workspace fingerprint does not match files"
        );
        request.spec.workspace_id = Some(id);
        self.submit_inner(request.spec, Some(request.workspace))
    }
    fn submit_inner(
        &mut self,
        spec: JobSpec,
        workspace: Option<crate::workspace::Bundle>,
    ) -> Result<Job> {
        spec.validate()?;
        if workspace.is_none()
            && let Some(id) = &spec.workspace_id
        {
            ensure!(
                self.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM workspaces WHERE id=?1)",
                    [id],
                    |r| r.get::<_, bool>(0)
                )?,
                "workspace not found; submit with --project to upload its files"
            );
        }
        let serialized = serde_json::to_string(&spec)?;
        if let Some(key) = &spec.submission_key {
            let old: Option<(String, String)> = self
                .conn
                .query_row(
                    "SELECT job_id,spec FROM submissions WHERE key=?1",
                    [key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((id, old_spec)) = old {
                ensure!(
                    serde_json::from_str::<JobSpec>(&old_spec)? == spec,
                    "submission key reused with a different specification"
                );
                return self.get(&id);
            }
        }
        let mut j = Job {
            id: Uuid::new_v4().to_string(),
            seq: 0,
            spec,
            state: JobState::Queued,
            submitted_ms: now_ms(),
            started_ms: None,
            finished_ms: None,
            attempt_id: None,
            exit_code: None,
            error: None,
            worker_id: None,
            attempts: 0,
            retry_at_ms: 0,
            lease_until_ms: 0,
            allocated_gpus: Vec::new(),
        };
        let tx = self.conn.transaction()?;
        if let Some(workspace) = workspace {
            tx.execute(
                "INSERT OR IGNORE INTO workspaces(id,data) VALUES(?1,?2)",
                params![j.spec.workspace_id, serde_json::to_vec(&workspace)?],
            )?;
        }
        tx.execute(
            "INSERT INTO jobs(id,state,data) VALUES (?1,'queued',?2)",
            params![j.id, serde_json::to_string(&j)?],
        )?;
        j.seq = tx.last_insert_rowid();
        Self::write(&tx, &j)?;
        if let Some(key) = &j.spec.submission_key {
            tx.execute(
                "INSERT INTO submissions(key,job_id,spec) VALUES (?1,?2,?3)",
                params![key, j.id, serialized],
            )?;
        }
        tx.commit()?;
        tracing::info!(job_id=%j.id,"job submitted");
        Ok(j)
    }
    pub fn list(&self) -> Result<Vec<Job>> {
        self.list_filtered(&JobFilter::default())
    }
    pub fn get(&self, id: &str) -> Result<Job> {
        let data: Option<String> = self
            .conn
            .query_row("SELECT data FROM jobs WHERE id=?1", [id], |r| r.get(0))
            .optional()?;
        Ok(serde_json::from_str(&data.context("job not found")?)?)
    }
    fn live(&self) -> Result<Vec<Job>> {
        let mut stmt=self.conn.prepare("SELECT data FROM jobs WHERE state IN ('queued','assigned','running','cancelling','uncertain') ORDER BY seq")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    fn reservations(&self) -> Result<Vec<Job>> {
        let mut stmt = self.conn.prepare("SELECT data FROM jobs WHERE state IN ('assigned','running','cancelling','uncertain') ORDER BY seq")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    pub fn attempt(&self, id: &str) -> Result<Attempt> {
        let data: Option<String> = self
            .conn
            .query_row("SELECT data FROM executions WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(serde_json::from_str(&data.context("attempt not found")?)?)
    }
    pub fn attempts(&self, id: &str) -> Result<Vec<Attempt>> {
        self.get(id)?;
        let mut stmt = self
            .conn
            .prepare("SELECT data FROM executions WHERE job_id=?1 ORDER BY rowid")?;
        let rows = stmt.query_map([id], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    pub fn cancel(&mut self, id: &str) -> Result<Job> {
        let mut j = self.get(id)?;
        match j.state {
            JobState::Queued | JobState::Assigned => {
                j.state = JobState::Cancelled;
                j.finished_ms = Some(now_ms());
                j.lease_until_ms = 0;
                let a = j.attempt_id.as_ref().and_then(|id| self.attempt(id).ok());
                let tx = self.conn.transaction()?;
                if let Some(mut a) = a {
                    a.state = AttemptState::Finished;
                    a.lease_until_ms = 0;
                    a.finished_ms = j.finished_ms;
                    a.error = Some("cancelled before execution".into());
                    Self::write_attempt(&tx, &a)?;
                }
                Self::write(&tx, &j)?;
                tx.commit()?;
            }
            JobState::Running | JobState::Uncertain => {
                j.state = JobState::Cancelling;
                Self::write(&self.conn, &j)?;
            }
            _ => {}
        }
        Ok(j)
    }
    pub fn expire(&mut self, now: i64) -> Result<()> {
        let jobs = {
            let mut stmt = self.conn.prepare("SELECT data FROM jobs WHERE state IN ('assigned','running','cancelling') AND lease_until_ms > 0 AND lease_until_ms <= ?1")?;
            let rows = stmt.query_map([now], |r| r.get::<_, String>(0))?;
            rows.map(|r| Ok(serde_json::from_str(&r?)?))
                .collect::<Result<Vec<Job>>>()?
        };
        let mut changes = Vec::new();
        for mut j in jobs
            .into_iter()
            .filter(|j| policy::active(j) && j.lease_until_ms <= now)
        {
            let Some(id) = &j.attempt_id else { continue };
            let Ok(mut a) = self.attempt(id) else {
                continue;
            };
            if a.state == AttemptState::Assigned {
                a.state = AttemptState::Finished;
                a.error = Some("dispatch lease expired before start".into());
                a.finished_ms = Some(now);
                j.state = JobState::Queued;
                j.worker_id = None;
            } else if a.state == AttemptState::Running {
                a.state = AttemptState::Uncertain;
                if j.state != JobState::Cancelling {
                    j.state = JobState::Uncertain;
                }
                j.error = Some("lease expired; execution must reconcile".into());
            } else {
                continue;
            }
            a.lease_until_ms = 0;
            j.lease_until_ms = 0;
            changes.push((j, a));
        }
        if changes.is_empty() {
            return Ok(());
        }
        let tx = self.conn.transaction()?;
        for (j, a) in changes {
            Self::write_attempt(&tx, &a)?;
            Self::write(&tx, &j)?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn claim(&mut self, auth: &WorkerAuth) -> Result<Option<Assignment>> {
        self.expire(now_ms())?;
        let w = self.worker(auth)?;
        ensure!(w.state == WorkerState::Online, "worker is not online");
        let assigned: Option<String> = self.conn.query_row(
            "SELECT data FROM jobs WHERE worker_id=?1 AND state='assigned' ORDER BY seq LIMIT 1", [&auth.id], |r| r.get(0)
        ).optional()?;
        if let Some(data) = assigned {
            let job: Job = serde_json::from_str(&data)?;
            return Ok(Some(Assignment {
                attempt_id: job
                    .attempt_id
                    .clone()
                    .context("assigned job lacks attempt")?,
                job,
            }));
        }
        let queued: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE state='queued')",
            [],
            |r| r.get(0),
        )?;
        if !queued {
            return Ok(None);
        }
        let workers = self.workers()?;
        if !workers.iter().any(|w| {
            w.state == WorkerState::Online
                && w.reserved.cpu < w.registration.capacity.cpu
                && w.reserved.mem_mib < w.registration.capacity.mem_mib
        }) {
            return Ok(None);
        }
        let mut jobs = self.live()?;
        let started = std::time::Instant::now();
        let placements = self.policy.plan(&jobs, &workers, now_ms());
        let elapsed = started.elapsed().as_nanos() as u64;
        self.metrics.passes += 1;
        self.metrics.policy_total_ns += elapsed;
        self.metrics.policy_max_ns = self.metrics.policy_max_ns.max(elapsed);
        self.metrics.placements += placements.len() as u64;
        let tx = self.conn.transaction()?;
        for p in placements {
            let j = jobs
                .iter_mut()
                .find(|j| j.id == p.job_id)
                .context("placement job missing")?;
            let w = workers
                .iter()
                .find(|w| w.registration.auth.id == p.worker_id)
                .context("placement worker missing")?;
            let a = Attempt {
                id: Uuid::new_v4().to_string(),
                job_id: j.id.clone(),
                auth: w.registration.auth.clone(),
                state: AttemptState::Assigned,
                lease_until_ms: now_ms() + LEASE_MS,
                exit_code: None,
                error: None,
                finished_ms: None,
                gpu_ids: p.gpu_ids.clone(),
                assigned_ms: now_ms(),
                started_ms: None,
            };
            j.allocated_gpus = p.gpu_ids;
            j.state = JobState::Assigned;
            j.attempt_id = Some(a.id.clone());
            j.worker_id = Some(p.worker_id);
            j.lease_until_ms = a.lease_until_ms;
            tx.execute(
                "INSERT INTO attempts(id,job_id) VALUES (?1,?2)",
                params![a.id, j.id],
            )?;
            Self::write_attempt(&tx, &a)?;
            Self::write(&tx, j)?;
        }
        let result = jobs
            .iter()
            .find(|j| j.state == JobState::Assigned && j.worker_id.as_deref() == Some(&auth.id))
            .map(|j| Assignment {
                job: j.clone(),
                attempt_id: j.attempt_id.clone().unwrap(),
            });
        tx.commit()?;
        Ok(result)
    }
    fn owned(&self, start: &Start) -> Result<Attempt> {
        self.worker(&start.auth)?;
        let a = self.attempt(&start.attempt_id)?;
        ensure!(
            a.job_id == start.job_id
                && a.auth.id == start.auth.id
                && a.auth.session == start.auth.session,
            "stale attempt or wrong worker"
        );
        Ok(a)
    }
    pub fn start(&mut self, start: Start) -> Result<Job> {
        self.expire(now_ms())?;
        let mut a = self.owned(&start)?;
        let mut j = self.get(&start.job_id)?;
        ensure!(
            j.attempt_id.as_deref() == Some(&a.id)
                && matches!(j.state, JobState::Assigned | JobState::Running)
                && a.lease_until_ms > now_ms(),
            "execution lease is not valid"
        );
        let first_start = a.state == AttemptState::Assigned;
        if first_start {
            j.attempts += 1;
            j.started_ms = Some(now_ms());
            a.started_ms = j.started_ms;
        }
        a.state = AttemptState::Running;
        a.lease_until_ms = now_ms() + LEASE_MS;
        j.state = JobState::Running;
        j.lease_until_ms = a.lease_until_ms;
        let tx = self.conn.transaction()?;
        Self::write_attempt(&tx, &a)?;
        Self::write(&tx, &j)?;
        tx.commit()?;
        if first_start {
            let dispatch = (j.started_ms.unwrap_or_default() - a.assigned_ms).max(0) as u64;
            self.metrics.starts += 1;
            self.metrics.queue_wait_total_ms +=
                (j.started_ms.unwrap_or_default() - j.submitted_ms).max(0) as u64;
            self.metrics.dispatch_total_ms += dispatch;
            self.metrics.dispatch_max_ms = self.metrics.dispatch_max_ms.max(dispatch);
            tracing::info!(job_id=%j.id,attempt_id=%a.id,worker_id=%a.auth.id,"execution started");
        }
        Ok(j)
    }
    fn output(conn: &Connection, attempt: &str, logs: &Logs, stopped: bool) -> Result<()> {
        ensure!(
            logs.stdout.len() <= LOG_LIMIT * 3 && logs.stderr.len() <= LOG_LIMIT * 3,
            "logs too large"
        );
        if logs.stdout.is_empty() && logs.stderr.is_empty() {
            return Ok(());
        }
        // Final UTF-8 decoding can be shorter than a partial codepoint's replacement character.
        conn.execute("UPDATE attempts SET stdout=CASE WHEN (?4 AND length(CAST(?1 AS BLOB))>0) OR length(CAST(?1 AS BLOB))>=length(CAST(stdout AS BLOB)) THEN ?1 ELSE stdout END,stderr=CASE WHEN (?4 AND length(CAST(?2 AS BLOB))>0) OR length(CAST(?2 AS BLOB))>=length(CAST(stderr AS BLOB)) THEN ?2 ELSE stderr END WHERE id=?3",params![logs.stdout,logs.stderr,attempt,stopped])?;
        Ok(())
    }
    pub fn logs(&self, id: &str) -> Result<Logs> {
        self.get(id)?;
        Ok(self
            .conn
            .query_row(
                "SELECT stdout,stderr,id FROM attempts WHERE job_id=?1 ORDER BY rowid DESC LIMIT 1",
                [id],
                |r| {
                    Ok(Logs {
                        stdout: r.get(0)?,
                        stderr: r.get(1)?,
                        attempt_id: Some(r.get(2)?),
                    })
                },
            )
            .optional()?
            .unwrap_or_default())
    }
    pub fn report(&mut self, r: Report) -> Result<Job> {
        self.expire(now_ms())?;
        let mut a = self.owned(&Start {
            auth: r.auth,
            job_id: r.job_id.clone(),
            attempt_id: r.attempt_id,
        })?;
        if a.state == AttemptState::Finished
            && !self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=?1)",
                [&r.job_id],
                |r| r.get::<_, bool>(0),
            )?
        {
            return Err(CompletionPruned.into());
        }
        let mut j = self.get(&r.job_id)?;
        if a.state == AttemptState::Finished {
            return Ok(j);
        }
        ensure!(
            j.attempt_id.as_deref() == Some(&a.id) && !j.state.terminal(),
            "stale attempt"
        );
        if let Some(code) = r.exit_code {
            a.state = AttemptState::Finished;
            a.exit_code = Some(code);
            a.error = r.error.clone();
            a.finished_ms = Some(now_ms());
            a.lease_until_ms = 0;
            j.exit_code = Some(code);
            j.error = r.error;
            j.lease_until_ms = 0;
            if j.state == JobState::Cancelling {
                j.state = JobState::Cancelled;
            } else if code == 0 {
                j.state = JobState::Succeeded;
            } else if a.started_ms.is_some() && j.attempts <= j.spec.max_retries {
                j.state = JobState::Queued;
                j.worker_id = None;
                j.started_ms = None;
                j.retry_at_ms = now_ms() + 1000 * (1i64 << (j.attempts.saturating_sub(1).min(6)));
            } else {
                j.state = JobState::Failed;
            }
            if j.state.terminal() {
                j.finished_ms = Some(now_ms());
            }
        } else if (a.state == AttemptState::Running && j.state == JobState::Running)
            || (a.state == AttemptState::Assigned && j.state == JobState::Assigned)
        {
            a.lease_until_ms = now_ms() + LEASE_MS;
            j.lease_until_ms = a.lease_until_ms;
        }
        let tx = self.conn.transaction()?;
        Self::output(&tx, &a.id, &r.logs, r.exit_code.is_some())?;
        Self::write_attempt(&tx, &a)?;
        Self::write(&tx, &j)?;
        tx.commit()?;
        if r.exit_code.is_some() {
            if j.state == JobState::Queued {
                self.metrics.retries += 1;
            }
            tracing::info!(job_id=%j.id,attempt_id=%a.id,worker_id=%a.auth.id,state=?j.state,exit_code=?j.exit_code,"stopped execution reported");
        }
        Ok(j)
    }
    pub fn resolve(&mut self, id: &str) -> Result<Job> {
        let j = self.get(id)?;
        ensure!(
            matches!(j.state, JobState::Uncertain | JobState::Cancelling),
            "only uncertain or cancelling executions can be resolved"
        );
        if let Some(attempt) = &j.attempt_id
            && let Ok(a) = self.attempt(attempt)
        {
            return self.report(Report {
                auth: a.auth,
                job_id: j.id.clone(),
                attempt_id: a.id,
                logs: Logs::default(),
                exit_code: Some(137),
                error: Some("operator confirmed execution stopped".into()),
            });
        }
        let mut j = j;
        j.state = JobState::Failed;
        j.finished_ms = Some(now_ms());
        Self::write(&self.conn, &j)?;
        Ok(j)
    }
    fn save_worker(&self, w: &Worker) -> Result<()> {
        self.conn.execute("INSERT INTO workers(id,data) VALUES (?1,?2) ON CONFLICT(id) DO UPDATE SET data=excluded.data",params![w.registration.auth.id,serde_json::to_string(w)?])?;
        Ok(())
    }
    pub fn worker(&self, auth: &WorkerAuth) -> Result<Worker> {
        let data: Option<String> = self
            .conn
            .query_row("SELECT data FROM workers WHERE id=?1", [&auth.id], |r| {
                r.get(0)
            })
            .optional()?;
        let w: Worker = serde_json::from_str(&data.context("worker not found")?)?;
        ensure!(
            w.registration.auth.session == auth.session,
            "stale worker session"
        );
        Ok(w)
    }
    pub fn register(&mut self, registration: Registration) -> Result<Worker> {
        registration.capacity.validate()?;
        registration.physical.validate()?;
        let ids: std::collections::BTreeSet<_> = registration.gpus.iter().map(|g| &g.id).collect();
        ensure!(
            ids.len() == registration.gpus.len()
                && ids.len() <= 64
                && registration.gpus.iter().all(|g| valid_gpu_id(&g.id)
                    && g.memory_mib > 0
                    && g.simulated == g.id.starts_with("sim:")),
            "invalid or duplicate GPU inventory"
        );
        for w in self.workers()? {
            if w.registration.auth.id != registration.auth.id {
                ensure!(
                    !w.registration.gpus.iter().any(|g| ids.contains(&g.id)),
                    "GPU identity is already advertised by another worker"
                );
            }
        }
        ensure!(
            registration.capacity.fits(registration.physical),
            "configured capacity exceeds physical resources"
        );
        ensure!(
            !registration.auth.id.is_empty()
                && registration.auth.id != "."
                && registration.auth.id != ".."
                && registration.auth.id.len() <= 64
                && registration
                    .auth
                    .id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
            "worker id must be 1..64 ASCII letters, digits, dots, dashes or underscores"
        );
        ensure!(
            Uuid::parse_str(&registration.auth.session).is_ok(),
            "invalid worker session"
        );
        let old: Option<String> = self
            .conn
            .query_row(
                "SELECT data FROM workers WHERE id=?1",
                [&registration.auth.id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            let old: Worker = serde_json::from_str(&old)?;
            if old.registration.auth.session != registration.auth.session {
                ensure!(
                    now_ms() - old.last_seen_ms > LEASE_MS * 2
                        && !self.reservations()?.iter().any(|j| policy::active(j)
                            && j.worker_id.as_deref() == Some(&registration.auth.id)),
                    "worker id is still owned by another session or has unreconciled executions"
                );
            } else {
                ensure!(
                    old.registration.capacity == registration.capacity
                        || !self.reservations()?.iter().any(|j| policy::active(j)
                            && j.worker_id.as_deref() == Some(&registration.auth.id)),
                    "cannot change capacity while executions need reconciliation"
                );
                let old_ids: std::collections::BTreeSet<_> =
                    old.registration.gpus.iter().map(|g| &g.id).collect();
                ensure!(
                    old_ids == ids
                        || !self.reservations()?.iter().any(|j| policy::active(j)
                            && j.worker_id.as_deref() == Some(&registration.auth.id)),
                    "cannot change GPUs while executions need reconciliation"
                );
            }
        }
        let w = Worker {
            registration,
            state: WorkerState::Online,
            last_seen_ms: now_ms(),
            reserved: Resources::default(),
            reserved_gpus: Vec::new(),
        };
        self.save_worker(&w)?;
        Ok(w)
    }
    pub fn stats(&self) -> Result<serde_json::Value> {
        let mut counts = std::collections::BTreeMap::<String, i64>::new();
        let mut stmt = self
            .conn
            .prepare("SELECT state,count(*) FROM jobs GROUP BY state")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            let (state, count) = row?;
            counts.insert(state, count);
        }
        let jobs: i64 = counts.values().sum();
        Ok(
            serde_json::json!({"schema_version":SCHEMA_VERSION,"policy":self.policy,"scheduling":self.metrics,"database":self.database_metrics,"jobs":jobs,"states":counts,"workers":self.workers()?,"measured_usage":"use benchmark workload/process telemetry; reservations are not measurements"}),
        )
    }
    pub fn heartbeat(&mut self, auth: &WorkerAuth, drain: bool) -> Result<Worker> {
        self.expire(now_ms())?;
        let mut w = self.worker(auth)?;
        w.last_seen_ms = now_ms();
        w.state = if drain {
            WorkerState::Draining
        } else {
            WorkerState::Online
        };
        self.save_worker(&w)?;
        Ok(w)
    }
    pub fn workers(&self) -> Result<Vec<Worker>> {
        let mut stmt = self.conn.prepare("SELECT data FROM workers ORDER BY id")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let jobs = self.reservations()?;
        rows.map(|row| {
            let mut w: Worker = serde_json::from_str(&row?)?;
            if now_ms() - w.last_seen_ms > LEASE_MS * 2 {
                w.state = WorkerState::Offline;
            }
            w.reserved = Resources::default();
            w.reserved_gpus.clear();
            for j in jobs.iter().filter(|j| {
                policy::active(j) && j.worker_id.as_deref() == Some(&w.registration.auth.id)
            }) {
                w.reserved.cpu += j.spec.resources.cpu;
                w.reserved.mem_mib += j.spec.resources.mem_mib;
                w.reserved_gpus.extend(j.allocated_gpus.iter().cloned());
            }
            Ok(w)
        })
        .collect()
    }
}
