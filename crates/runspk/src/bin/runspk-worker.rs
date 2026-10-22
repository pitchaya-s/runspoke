use clap::Parser;
use runspk::{client::Client, model::*, runner};

#[derive(Parser)]
#[command(name = "runspk-worker", version)]
struct Args {
    #[command(flatten)]
    settings: runspk::config::SettingsArgs,
    /// Advertise a worker capability (repeatable KEY=VALUE).
    #[arg(long="label",env="RUNSPK_LABELS",value_delimiter=',',value_parser=runspk::model::parse_label)]
    labels: Vec<(String, String)>,
    #[arg(long, env = "RUNSPK_URL")]
    url: Option<String>,
    #[arg(long, env = "RUNSPK_TOKEN", hide_env_values = true)]
    token: Option<String>,
    #[arg(long, env = "RUNSPK_CONFIG")]
    config: Option<std::path::PathBuf>,
    #[arg(env = "RUNSPK_WORKER_ID", long)]
    id: String,
    #[arg(env = "RUNSPK_CPU", long)]
    cpu: Option<u32>,
    #[arg(env = "RUNSPK_MEM_MIB", long)]
    mem_mib: Option<u64>,
    #[arg(env = "RUNSPK_ALLOW_NATIVE", long)]
    allow_native: bool,
    #[arg(env = "RUNSPK_DATA_DIR", long)]
    data_dir: Option<std::path::PathBuf>,
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
    let settings = a.settings.load()?;
    // SAFETY: geteuid has no pointer or lifetime preconditions.
    anyhow::ensure!(
        !a.allow_native || unsafe { libc::geteuid() } != 0,
        "native execution must use an unprivileged worker account"
    );
    let config = runspk::profile::config_path(a.config)?;
    let profile = runspk::profile::Profile::load(&config)?;
    let url = a
        .url
        .or_else(|| profile.as_ref().map(|p| p.url.clone()))
        .unwrap_or_else(|| "http://127.0.0.1:8765".into());
    let token = a
        .token
        .or_else(|| profile.as_ref().map(|p| p.token.clone()))
        .ok_or_else(|| anyhow::anyhow!("run runspk init / runspk join, or set RUNSPK_TOKEN"))?;
    let data_dir = match a.data_dir {
        Some(path) => path,
        None => match &profile {
            Some(p) => p.state()?.join("workers"),
            None => ".runspk/workers".into(),
        },
    };
    if let Some(profile) = &profile {
        runspk::profile::private_dir(&profile.state()?)?;
    }
    let client = Client::new(url, token)?.with_limits(settings.transfers)?;
    let journal = std::sync::Arc::new(runspk::journal::Journal::open(&data_dir, &a.id)?);
    let r = Registration {
        auth: journal.auth.clone(),
        capacity: Resources {
            cpu: a.cpu.unwrap_or(settings.worker.cpu),
            mem_mib: a.mem_mib.unwrap_or(settings.worker.mem_mib),
        },
        physical: runner::physical_resources()?,
        allow_native: a.allow_native,
        runtime: a.runtime,
        gpus: runspk::hardware::select_gpus(&a.gpu_ids, a.simulate_gpus, &a.id)?,
        project_staging: true,
        labels: settings
            .worker
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(a.labels)
            .collect(),
        transfer_limits: Some(settings.transfers),
    };
    let (tx, rx) = tokio::sync::watch::channel(false);
    let worker = runner::worker(client, r, journal, rx);
    tokio::pin!(worker);
    tokio::select! {result=&mut worker=>result?,_=runspk::shutdown_signal()=>{let _=tx.send(true);worker.await?;}}
    Ok(())
}
