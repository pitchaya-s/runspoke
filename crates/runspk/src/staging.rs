//! Assignment preparation renews permission to stage, never permission to execute.
use crate::{
    client::Client,
    journal::{Entry, Journal},
    model::*,
    workspace::Bundle,
};
use anyhow::{Context, Result, ensure};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};

pub(crate) async fn prepare(
    client: &Client,
    journal: &Arc<Journal>,
    entry: &Entry,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<PathBuf>> {
    let Some(id) = &entry.assignment.job.spec.workspace_id else {
        return Ok(None);
    };
    let path = journal.workspace_path(&entry.assignment.attempt_id)?;
    let destination = path.clone();
    let transfer = async {
        let limits = entry
            .assignment
            .job
            .spec
            .transfer_limits
            .unwrap_or_default();
        ensure!(
            client.limits.supports(&limits),
            "worker transfer limits are too small for assigned project; upgrade worker settings"
        );
        let bundle: Bundle = client
            .get_with_timeout(&format!("/workspaces/{id}"), client.transfer_timeout())
            .await?;
        ensure!(
            bundle.id_with(limits)? == *id,
            "downloaded workspace fingerprint mismatch"
        );
        journal
            .blocking(move |_| {
                let workspaces = destination.parent().context("missing workspace parent")?;
                std::fs::create_dir_all(workspaces)?;
                std::fs::File::open(workspaces.parent().context("missing journal directory")?)?
                    .sync_all()?;
                crate::workspace::materialize_with(&bundle, &destination, limits, false)
            })
            .await
    };
    tokio::pin!(transfer);
    let mut deadline = Instant::now() + Duration::from_secs(3);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _=shutdown.changed()=>anyhow::bail!("worker stopped while preparing project"),
            _=tokio::time::sleep_until(deadline)=>anyhow::bail!("assignment lease expired while preparing project"),
            _=tick.tick()=> {
                let sent=Instant::now();
                let report=Report{auth:journal.auth.clone(),job_id:entry.assignment.job.id.clone(),attempt_id:entry.assignment.attempt_id.clone(),logs:Logs::default(),exit_code:None,error:None};
                match tokio::time::timeout_at(deadline,client.post::<Job>("/worker/report",&report)).await {
                    Ok(Ok(job))=>{ensure!(job.state==JobState::Assigned,"project preparation cancelled or fenced");deadline=sent+Duration::from_secs(3);}
                    Ok(Err(e))=>tracing::debug!(error=%e,"preparation lease renewal failed"),
                    Err(_)=>anyhow::bail!("assignment lease expired while preparing project"),
                }
            }
            result=&mut transfer=>{result.context("prepare project inputs")?;return Ok(Some(path));}
        }
    }
}

pub(crate) async fn collect(
    _client: &Client,
    journal: &Arc<Journal>,
    entry: &Entry,
) -> Result<Option<Bundle>> {
    if entry.assignment.job.spec.workspace_id.is_none()
        || entry.assignment.job.spec.outputs.is_empty()
        || entry.process.is_none()
    {
        return Ok(None);
    }
    let path = journal.workspace_path(&entry.assignment.attempt_id)?;
    let outputs = entry.assignment.job.spec.outputs.clone();
    let limits = entry
        .assignment
        .job
        .spec
        .transfer_limits
        .unwrap_or_default();
    let owner = Start {
        auth: journal.auth.clone(),
        job_id: entry.assignment.job.id.clone(),
        attempt_id: entry.assignment.attempt_id.clone(),
    };
    journal
        .blocking(move |_| {
            let bundle = crate::workspace::capture_outputs_with(&path, &outputs, limits)?;
            crate::workspace::check_artifact_size(&owner, &bundle, limits.http_body_bytes.0)?;
            Ok(bundle)
        })
        .await
        .map(Some)
}
