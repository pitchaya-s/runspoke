//! Initial schema; generated columns cannot drift from job records.
use anyhow::{Context, Result, ensure};
use rusqlite::Connection;

pub const SCHEMA_VERSION: u32 = 1;
const JOBS_TABLE: &str = "CREATE TABLE jobs(
                seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,data TEXT NOT NULL,
                lease_until_ms INTEGER GENERATED ALWAYS AS (coalesce(json_extract(data,'$.lease_until_ms'),0)) VIRTUAL,
                worker_id TEXT GENERATED ALWAYS AS (json_extract(data,'$.worker_id')) VIRTUAL,
                finished_ms INTEGER GENERATED ALWAYS AS (json_extract(data,'$.finished_ms')) VIRTUAL,
                workspace_id TEXT GENERATED ALWAYS AS (json_extract(data,'$.spec.workspace_id')) VIRTUAL)";

pub(super) fn initialize(conn: &mut Connection) -> Result<()> {
    let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    ensure!(
        version <= SCHEMA_VERSION,
        "incompatible database schema {version}; use a matching runspk binary or restore a compatible backup"
    );
    if version == 0 {
        ensure!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'",
                [],
                |r| r.get::<_, u32>(0)
            )? == 0,
            "unrecognized database layout; choose a new state path or restore a compatible backup"
        );
        let tx = conn.transaction()?;
        tx.execute_batch(JOBS_TABLE)?;
        tx.execute_batch("CREATE INDEX jobs_state ON jobs(state,seq);
            CREATE INDEX jobs_lease ON jobs(state,lease_until_ms) WHERE state IN ('assigned','running','cancelling');
            CREATE INDEX jobs_worker_state ON jobs(worker_id,state,seq);
            CREATE INDEX jobs_finished ON jobs(finished_ms,seq) WHERE state IN ('succeeded','failed','cancelled');
            CREATE INDEX jobs_workspace ON jobs(workspace_id);
            CREATE TABLE workers(id TEXT PRIMARY KEY,data TEXT NOT NULL);
            CREATE TABLE attempts(id TEXT PRIMARY KEY,job_id TEXT NOT NULL,stdout TEXT NOT NULL DEFAULT '',stderr TEXT NOT NULL DEFAULT '');
            CREATE INDEX attempts_job ON attempts(job_id);
            CREATE TABLE executions(id TEXT PRIMARY KEY,job_id TEXT NOT NULL,data TEXT NOT NULL);
            CREATE INDEX executions_job ON executions(job_id);
            CREATE TABLE submissions(key TEXT PRIMARY KEY,job_id TEXT NOT NULL,spec TEXT NOT NULL);
            CREATE INDEX submissions_job ON submissions(job_id);
            CREATE TABLE workspaces(id TEXT PRIMARY KEY,data BLOB NOT NULL);
            CREATE TABLE artifacts(attempt_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,data BLOB NOT NULL);
            CREATE INDEX artifacts_job ON artifacts(job_id);")?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
    } else {
        // Lease and pruning decisions require the exact generated expressions, not just column names.
        let jobs_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'",
                [],
                |r| r.get(0),
            )
            .context("incompatible database layout; restore a compatible backup")?;
        ensure!(
            jobs_sql == JOBS_TABLE,
            "incompatible database layout; restore a compatible backup instead of changing its schema version"
        );
        conn.prepare("SELECT jobs.seq,jobs.id,jobs.state,jobs.data,jobs.lease_until_ms,jobs.worker_id,jobs.finished_ms,jobs.workspace_id,
            workers.id,workers.data,attempts.id,attempts.job_id,attempts.stdout,attempts.stderr,
            executions.id,executions.job_id,executions.data,submissions.key,submissions.job_id,submissions.spec,
            workspaces.id,workspaces.data,artifacts.attempt_id,artifacts.job_id,artifacts.data
            FROM jobs,workers,attempts,executions,submissions,workspaces,artifacts LIMIT 0")
            .context("incompatible database layout; restore a compatible backup instead of changing its schema version")?;
    }
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
    Ok(())
}
