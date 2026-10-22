use anyhow::{Context, Result, ensure};
use clap::Args;
use runspk::{
    client::Client,
    profile::{self, Profile},
};
use std::{
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

#[derive(Args)]
pub(super) struct StartArgs {
    /// Run only the scheduler; attach workers separately.
    #[arg(long)]
    pub scheduler_only: bool,
    #[arg(long, env = "RUNSPK_CPU")]
    pub cpu: Option<u32>,
    #[arg(long,env="RUNSPK_MEM_MIB",value_parser=super::parse_mem)]
    pub mem: Option<u64>,
    #[arg(long, value_delimiter = ',')]
    pub gpu_ids: Vec<String>,
    #[arg(long, default_value_t = 0)]
    pub simulate_gpus: u32,
    #[arg(long="label",env="RUNSPK_LABELS",value_delimiter=',',value_parser=runspk::model::parse_label)]
    pub labels: Vec<(String, String)>,
}
#[derive(Args)]
pub(super) struct JoinArgs {
    /// Private connection invitation copied over SSH.
    pub invitation: PathBuf,
    #[arg(long)]
    pub id: String,
    #[arg(long, env = "RUNSPK_CPU")]
    pub cpu: Option<u32>,
    #[arg(long,env="RUNSPK_MEM_MIB",value_parser=super::parse_mem)]
    pub mem: Option<u64>,
    /// Allow trusted jobs to run as your user on this machine.
    #[arg(long, required = true)]
    pub allow_native: bool,
    #[arg(long, value_delimiter = ',')]
    pub gpu_ids: Vec<String>,
    #[arg(long, default_value_t = 0)]
    pub simulate_gpus: u32,
    #[arg(long="label",env="RUNSPK_LABELS",value_delimiter=',',value_parser=runspk::model::parse_label)]
    pub labels: Vec<(String, String)>,
}
pub(super) fn companion(name: &str) -> Result<PathBuf> {
    let path = std::env::current_exe()?
        .parent()
        .context("binary directory missing")?
        .join(name);
    ensure!(
        path.is_file(),
        "{name} is missing beside runspk; install the complete runspk package"
    );
    let version = env!("CARGO_PKG_VERSION");
    let output = Command::new("/usr/bin/timeout")
        .args(["--foreground", "--signal=KILL", "3s"])
        .arg(&path)
        .arg("--version")
        .env_remove("RUNSPK_TOKEN")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .with_context(|| {
            format!("cannot run /usr/bin/timeout to check {name} --version; install GNU coreutils")
        })?;
    ensure!(
        !matches!(output.status.code(), Some(124 | 137)),
        "{name} --version timed out or was killed (3s limit); reinstall the complete runspk {version} package"
    );
    ensure!(
        output.status.success()
            && std::str::from_utf8(&output.stdout)
                .is_ok_and(|s| s.trim() == format!("{name} {version}")),
        "{name} is incompatible with runspk {version}: expected `{name} {version}` from --version; reinstall the complete runspk {version} package"
    );
    Ok(path)
}
pub(super) fn init(
    path: &Path,
    url: Option<String>,
    state: Option<PathBuf>,
) -> Result<serde_json::Value> {
    if let Some(existing) = Profile::load(path)? {
        ensure!(
            url.as_ref().is_none_or(|url| url == &existing.url)
                && state
                    .as_ref()
                    .is_none_or(|state| Some(state) == existing.state_dir.as_ref()),
            "configuration already exists; choose --config with another path to initialize a different scheduler"
        );
        return Ok(serde_json::json!({"config":path,"created":false}));
    }
    Profile::local(url.unwrap_or_else(|| "http://127.0.0.1:8765".into()), state)?.save(path)?;
    Ok(serde_json::json!({"config":path,"created":true}))
}
pub(super) fn start(
    profile: &Profile,
    args: StartArgs,
    settings: &runspk::config::SettingsArgs,
) -> Result<()> {
    let defaults = settings.load()?.worker;
    let url = reqwest::Url::parse(&profile.url)?;
    ensure!(
        url.scheme() == "http" && url.path() == "/",
        "runspk start needs a local HTTP URL; HTTPS endpoints are served by your TLS proxy"
    );
    let ip: std::net::IpAddr = if url.host_str() == Some("localhost") {
        "127.0.0.1".parse()?
    } else {
        url.host_str()
            .context("URL needs a host")?
            .trim_matches(['[', ']'])
            .parse()?
    };
    ensure!(
        ip.is_loopback(),
        "runspk start binds loopback; use SSH forwarding or an HTTPS proxy"
    );
    let state = profile.state()?;
    profile::private_dir(&state)?;
    let mut command = Command::new(companion("runspk-scheduler")?);
    command
        .args([
            "--bind",
            &std::net::SocketAddr::new(
                ip,
                url.port_or_known_default().context("URL needs a port")?,
            )
            .to_string(),
        ])
        .arg("--db")
        .arg(state.join("scheduler.sqlite"))
        .env("RUNSPK_TOKEN", &profile.token);
    if !args.scheduler_only {
        command.args([
            "--local",
            "--cpu",
            &args.cpu.unwrap_or(defaults.cpu).to_string(),
            "--mem-mib",
            &args.mem.unwrap_or(defaults.mem_mib).to_string(),
            "--simulate-gpus",
            &args.simulate_gpus.to_string(),
        ]);
    }
    if !args.gpu_ids.is_empty() {
        command.arg("--gpu-ids").arg(args.gpu_ids.join(","));
    }
    eprintln!(
        "runspk: serving {} (state {})",
        profile.url,
        state.display()
    );
    for (key, value) in args.labels {
        command.arg("--label").arg(format!("{key}={value}"));
    }
    settings.apply(&mut command)?;
    Err(command.exec()).context("start scheduler")
}
pub(super) fn join(
    path: &Path,
    args: JoinArgs,
    settings: &runspk::config::SettingsArgs,
) -> Result<()> {
    let defaults = settings.load()?.worker;
    let invitation = Profile::load(&args.invitation)?
        .context("invitation not found; copy the private file from runspk invite")?;
    let profile = Profile {
        state_dir: None,
        ..invitation
    };
    if let Some(existing) = Profile::load(path)? {
        ensure!(
            existing.url == profile.url && existing.token == profile.token,
            "another connection is already configured; pass --config with a new private file"
        );
    } else {
        profile.save(path)?;
    }
    profile::private_dir(&profile.state()?)?;
    let mut command = Command::new(companion("runspk-worker")?);
    command
        .arg("--config")
        .arg(path)
        .env("RUNSPK_URL", &profile.url)
        .env("RUNSPK_TOKEN", &profile.token)
        .args([
            "--id",
            &args.id,
            "--cpu",
            &args.cpu.unwrap_or(defaults.cpu).to_string(),
            "--mem-mib",
            &args.mem.unwrap_or(defaults.mem_mib).to_string(),
            "--allow-native",
            "--simulate-gpus",
            &args.simulate_gpus.to_string(),
        ]);
    if !args.gpu_ids.is_empty() {
        command.arg("--gpu-ids").arg(args.gpu_ids.join(","));
    }
    eprintln!(
        "runspk: joining {} as {}; keep this worker supervised",
        profile.url, args.id
    );
    for (key, value) in args.labels {
        command.arg("--label").arg(format!("{key}={value}"));
    }
    settings.apply(&mut command)?;
    Err(command.exec()).context("start worker")
}
pub(super) async fn doctor(
    path: &Path,
    url: Option<String>,
    token: Option<String>,
) -> Result<(bool, serde_json::Value)> {
    let mut checks = Vec::new();
    let profile = Profile::load(path);
    checks.push(serde_json::json!({"check":"configuration","ok":profile.as_ref().is_ok_and(|saved| saved.is_some() || token.is_some()),"detail":match &profile {Ok(Some(_))=>format!("private config {}",path.display()),Ok(None)=>"Run runspk init, or provide RUNSPK_TOKEN and RUNSPK_URL".into(),Err(e)=>format!("{e:#}")}}));
    let profile = profile.ok().flatten();
    for name in ["runspk-scheduler", "runspk-worker"] {
        let result = companion(name);
        checks.push(serde_json::json!({"check":name,"ok":result.is_ok(),"detail":result.map(|p|p.display().to_string()).unwrap_or_else(|e|e.to_string())}));
    }
    checks.push(serde_json::json!({"check":"native-timeout","ok":Path::new("/usr/bin/timeout").is_file(),"detail":"Native workers require GNU coreutils /usr/bin/timeout"}));
    let url = url
        .or_else(|| profile.as_ref().map(|p| p.url.clone()))
        .unwrap_or_else(|| "http://127.0.0.1:8765".into());
    let token = token.or_else(|| profile.map(|p| p.token));
    let connection = async {
        let client = Client::new(url, token.clone().context("initialize first with runspk init")?)?;
        let info = client.get::<serde_json::Value>("/info").await?;
        let version = env!("CARGO_PKG_VERSION");
        ensure!(
            info["version"].as_str() == Some(version),
            "scheduler version is incompatible with runspk {version}; install runspk {version} on the scheduler and restart it"
        );
        let schema = runspk::store::SCHEMA_VERSION;
        ensure!(
            info["schema_version"] == schema,
            "scheduler protocol is incompatible: expected schema_version {schema}; install runspk {version} on the scheduler and restart it"
        );
        for feature in ["project-snapshots-v1", "artifacts-v1"] {
            ensure!(
                info["features"].as_array().is_some_and(|features| features.iter().any(|v| v == feature)),
                "scheduler capabilities are incompatible: missing {feature}; install runspk {version} on the scheduler and restart it"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let mut detail = connection
        .as_ref()
        .map(|_| format!("connected to runspk {}", env!("CARGO_PKG_VERSION")))
        .unwrap_or_else(|e| format!("{e:#}; run runspk start or check the SSH tunnel/token"));
    if let Some(token) = token.filter(|token| !token.is_empty()) {
        detail = detail.replace(&token, "[redacted]");
    }
    checks.push(serde_json::json!({"check":"scheduler","ok":connection.is_ok(),"detail":detail}));
    let ok = checks.iter().all(|c| c["ok"] == true);
    Ok((ok, serde_json::json!({"healthy":ok,"checks":checks})))
}
