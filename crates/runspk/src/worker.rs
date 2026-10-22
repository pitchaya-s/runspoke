//! Worker registration, recovery, polling and completion replay.
//! Execution tasks retain CPU ownership on errors until a worker restart reconciles them.
use crate::{
    client::Client,
    journal::Journal,
    model::*,
    runner::{execute, publish},
};
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet};

async fn recover(client: &Client, journal: &Arc<Journal>) -> Result<()> {
    for id in journal.ids().await? {
        let Some(mut entry) = journal.entry(&id).await? else {
            continue;
        };
        if entry.completion.is_none() {
            if let Some(process) = &entry.process {
                process.stop().await?;
            }
            crate::backend::cleanup(&entry).await?;
            let mut error = "worker restarted; prior process stopped".to_owned();
            match crate::staging::collect(client, journal, &entry).await {
                Ok(bundle) => entry.artifacts = bundle,
                Err(e) => {
                    entry.keep_workspace = true;
                    error.push_str(&format!("; outputs retained on worker: {e:#}"));
                }
            }
            entry.completion = Some(Report {
                auth: journal.auth.clone(),
                job_id: entry.assignment.job.id.clone(),
                attempt_id: entry.assignment.attempt_id.clone(),
                logs: Logs::default(),
                exit_code: Some(137),
                error: Some(error),
            });
            journal.save(&entry).await?;
        }
    }
    Ok(())
}
pub async fn worker(
    client: Client,
    registration: Registration,
    journal: Arc<Journal>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    registration.validate_metadata()?;
    registration.capacity.validate()?;
    anyhow::ensure!(
        registration.capacity.fits(registration.physical),
        "configured capacity exceeds physical resources"
    );
    anyhow::ensure!(
        registration.allow_native || registration.runtime.is_some(),
        "enable --allow-native or --runtime"
    );
    anyhow::ensure!(
        std::path::Path::new("/usr/bin/timeout").is_file(),
        "GNU coreutils /usr/bin/timeout is required"
    );
    // Fence recorded processes before registration: a changed capacity must not
    // prevent the old session from reporting that its executions have stopped.
    recover(&client, &journal).await?;
    if let Some(runtime) = registration.runtime {
        crate::backend::check(runtime).await?;
    }
    let pool = crate::hardware::cpu_ids()?;
    anyhow::ensure!(
        pool.len() >= registration.capacity.cpu as usize,
        "CPU budget exceeds worker affinity"
    );
    let pool = &pool[..registration.capacity.cpu as usize];
    loop {
        for id in journal.ids().await? {
            if let Some(entry) = journal.entry(&id).await? {
                publish(&client, &journal, &entry).await?;
            }
        }
        match client
            .post::<Worker>("/worker/register", &registration)
            .await
        {
            Ok(_) => break,
            Err(e) => {
                tracing::warn!(error=%e,"registration failed; retrying");
                tokio::select! {_=shutdown.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            }
        }
    }
    let telemetry = crate::telemetry::subscribe();
    let runtime = registration.runtime;
    let auth = registration.auth;
    let mut tasks = JoinSet::new();
    let mut replay = JoinSet::new();
    let mut active = std::collections::BTreeMap::<String, Vec<usize>>::new();
    let mut pending: std::collections::BTreeSet<_> = journal.ids().await?.into_iter().collect();
    let mut maintenance = tokio::time::interval(Duration::from_secs(1));
    let mut last_replayed = None;
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _=shutdown.changed()=>break,
            Some(result)=replay.join_next(),if !replay.is_empty()=> {
                match result {
                    Ok(Ok(last))=>last_replayed=last,
                    other=>tracing::warn!(?other,"completion replay retained for another attempt"),
                }
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Ok(Ok(id)) => { active.remove(&id); pending.insert(id); }
                    other => tracing::error!(worker_id=%auth.id,?other,"execution retained for reconciliation; restart worker"),
                }
            }
            _=maintenance.tick()=> {
                // Active executions publish their own completion; replay owns only finished tasks.
                let heartbeat=Heartbeat{auth:auth.clone(),telemetry:telemetry.borrow().clone()};
                if let Err(e) = client.post::<Worker>("/worker/heartbeat", &heartbeat).await {
                    tracing::debug!(error=%e,"heartbeat failed");
                }
                let mut entries=journal.ids().await?;
                entries.retain(|id|!active.contains_key(id));
                pending.clear();
                pending.extend(entries.iter().cloned());
                let offset=entries.partition_point(|entry|last_replayed.as_ref().is_some_and(|id|entry<=id));
                entries.rotate_left(offset);
                if replay.is_empty() && !entries.is_empty() {
                    let client=client.clone();let journal=journal.clone();
                    // One background transfer at a time; slow outputs cannot block heartbeats.
                    replay.spawn(async move {
                        let started=std::time::Instant::now();
                        let mut last=None;
                        for id in entries.into_iter().take(8) {
                            let _artifact_permit=client.artifact_permit().await?;
                            last=Some(id.clone());
                            if let Some(entry)=journal.entry(&id).await? {publish(&client,&journal,&entry).await?;}
                            if started.elapsed()>=Duration::from_millis(200){break;}
                        }
                        Ok::<_,anyhow::Error>(last)
                    });
                }
            }
            _=tick.tick()=> {
                match client.post::<Option<Assignment>>("/worker/poll",&auth).await {
                    Ok(Some(a)) if !active.contains_key(&a.attempt_id) && !pending.contains(&a.attempt_id) => {
                        let used: std::collections::BTreeSet<_> = active.values().flatten().copied().collect();
                        let cpus: Vec<_> = pool.iter().copied().filter(|cpu|!used.contains(cpu)).take(a.job.spec.resources.cpu as usize).collect();
                        if cpus.len() == a.job.spec.resources.cpu as usize {
                            active.insert(a.attempt_id.clone(),cpus.clone());
                            tasks.spawn(execute(client.clone(),journal.clone(),runtime,a,cpus,shutdown.clone()));
                        }
                    }
                    Ok(_) => {},
                    Err(e) => tracing::debug!(error=%e,"poll failed"),
                }
            }
        }
    }
    // Replay contains only durable stopped completions; cancellation is safe and idempotent.
    replay.abort_all();
    while replay.join_next().await.is_some() {}
    let _ = client.post::<Worker>("/worker/drain", &auth).await;
    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result? {
            tracing::error!(error=%e,"execution shutdown requires reconciliation");
        }
    }
    Ok(())
}
