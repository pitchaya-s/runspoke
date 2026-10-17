use clap::Parser;
use runspk::{api, client::Client, model::*, runner, store::Store};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Parser)]
#[command(name = "runspk-scheduler", version)]
struct Args {
    #[arg(env = "RUNSPK_BIND", long, default_value = "127.0.0.1:8765")]
    bind: SocketAddr,
    #[arg(env = "RUNSPK_DB", long, default_value = ".runspk/scheduler.sqlite")]
    db: PathBuf,
    #[arg(long, env = "RUNSPK_TOKEN", hide_env_values = true)]
    token: String,
    #[arg(long)]
    local: bool,
    #[arg(env = "RUNSPK_CPU", long, default_value_t = 1)]
    cpu: u32,
    #[arg(env = "RUNSPK_MEM_MIB", long, default_value_t = 256)]
    mem_mib: u64,
    #[arg(env = "RUNSPK_POLICY", long, value_enum, default_value = "fifo")]
    policy: runspk::policy::Policy,
    #[arg(env = "RUNSPK_RUNTIME", long, value_enum)]
    runtime: Option<ContainerRuntime>,
    #[arg(env = "RUNSPK_GPU_IDS", long, value_delimiter = ',')]
    gpu_ids: Vec<String>,
    #[arg(env = "RUNSPK_SIMULATE_GPUS", long, default_value_t = 0)]
    simulate_gpus: u32,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    // SAFETY: umask takes a valid permission mask, before project files or jobs are created.
    unsafe {
        libc::umask(0o077);
    }
    runspk::init_logging();
    let a = Args::parse();
    // SAFETY: geteuid has no pointer or lifetime preconditions.
    anyhow::ensure!(
        !a.local || unsafe { libc::geteuid() } != 0,
        "native execution must use an unprivileged worker account"
    );
    anyhow::ensure!(
        a.bind.ip().is_loopback(),
        "bind loopback; use an SSH tunnel for remote clients"
    );
    if let Some(p) = a.db.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(p)?;
    }
    let mut store = Store::open(&a.db)?;
    store.policy = a.policy;
    let store = Arc::new(Mutex::new(store));
    let router = api::router(store.clone(), a.token.clone())?;
    let listener = tokio::net::TcpListener::bind(a.bind).await?;
    tracing::info!(address=%listener.local_addr()?,"scheduler listening");
    let (stop, rx) = tokio::sync::watch::channel(false);
    let mut maintenance_stop = rx.clone();
    let maintenance_store = store.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            tokio::select! {_=maintenance_stop.changed()=>break,_=tick.tick()=>{if let Err(e)=api::db(maintenance_store.clone(),|s|s.expire(now_ms())).await {tracing::error!(error=%e,"lease maintenance failed");}}}
        }
    });
    let task = if a.local {
        let journal = Arc::new(runspk::journal::Journal::open(
            &a.db
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join("workers"),
            "local",
        )?);
        Some(tokio::spawn(runner::worker(
            Client::new(format!("http://{}", listener.local_addr()?), a.token)?,
            Registration {
                auth: journal.auth.clone(),
                capacity: Resources {
                    cpu: a.cpu,
                    mem_mib: a.mem_mib,
                },
                physical: runner::physical_resources()?,
                allow_native: true,
                runtime: a.runtime,
                gpus: runspk::hardware::select_gpus(&a.gpu_ids, a.simulate_gpus, "local")?,
                project_staging: true,
            },
            journal,
            rx,
        )))
    } else {
        None
    };
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let result = async {
                if let Some(mut task) = task {
                    tokio::select! {
                        result=&mut task=>result?,
                        _=runspk::shutdown_signal()=>{let _=stop.send(true);task.await??;Ok(())}
                    }
                } else {
                    runspk::shutdown_signal().await;
                    Ok::<(), anyhow::Error>(())
                }
            }
            .await;
            let _ = stop.send(true);
            let _ = finished_tx.send(result);
        })
        .await?;
    finished_rx.await??;
    Ok(())
}
