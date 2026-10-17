#[path = "runspk/display.rs"]
mod display;
#[path = "runspk/setup.rs"]
mod setup;
#[path = "runspk/submit.rs"]
mod submit;
use clap::{CommandFactory, Parser, Subcommand};
use runspk::profile::{self, Profile};
use runspk::{client::Client, model::*};
use std::{
    io::{IsTerminal, Write},
    path::PathBuf,
};
use submit::{SubmitArgs, submit};

#[derive(Parser)]
#[command(
    version,
    about = "Your machines. One compute queue.",
    after_help = "Start here: runspk init, then runspk start.
Project: runspk run --project . --output results --trusted -- python3 main.py
Terminal output is human-readable; pipes retain JSON. Use --json or --human explicitly."
)]
struct Args {
    #[arg(long, env = "RUNSPK_URL", global = true)]
    url: Option<String>,
    #[arg(long, env = "RUNSPK_TOKEN", hide_env_values = true, global = true)]
    token: Option<String>,
    #[arg(long, global = true)]
    json: bool,
    /// Use a private JSON connection profile (defaults to the XDG config directory).
    #[arg(long, env = "RUNSPK_CONFIG", global = true)]
    config: Option<PathBuf>,
    /// Force terminal tables and explanations even when stdout is redirected.
    #[arg(long, global = true, conflicts_with = "json")]
    human: bool,
    #[command(subcommand)]
    command: Action,
}
#[derive(Subcommand)]
enum Action {
    /// Create private local connection settings and a random token.
    Init {
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Start in the foreground; supervise or background explicitly for disconnected use.
    Start(setup::StartArgs),
    /// Export a private invitation granting administrative scheduler access.
    Invite {
        #[arg(long)]
        out: PathBuf,
    },
    /// Import an invitation and start a trusted worker in the foreground.
    Join(setup::JoinArgs),
    /// Check configuration, installed companion binaries and scheduler connectivity.
    Doctor,
    /// Generate completions; does not modify your shell configuration.
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Submit detached work and print its durable job ID.
    Submit(SubmitArgs),
    /// Submit, stream output, and return the workload's exit code.
    Run {
        #[command(flatten)]
        spec: SubmitArgs,
        #[arg(long)]
        wait_timeout: Option<u64>,
    },
    /// Wait for a terminal state; interruption leaves the job running.
    Wait {
        id: String,
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Inspect job history with indexed filters and stable sequence cursors.
    List(JobFilter),
    /// Show waiting jobs and their resource requests.
    Queue(JobFilter),
    /// Explain worker compatibility, resource reservations and retry backoff.
    Explain {
        id: String,
    },
    /// Delete at most --limit old, unkeyed terminal jobs and their history.
    Prune {
        #[arg(long)]
        before: i64,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: u32,
        #[arg(long)]
        dry_run: bool,
    },
    /// Back up a local scheduler database while it is running (no API token needed).
    Backup {
        #[arg(long, default_value = ".runspk/scheduler.sqlite")]
        db: std::path::PathBuf,
        #[arg(long)]
        out: std::path::PathBuf,
    },
    /// Retrieve a completed attempt's declared outputs into a new directory.
    Fetch {
        id: String,
        #[arg(long)]
        into: PathBuf,
        /// Attempt UUID from `runspk attempts JOB_ID`; omit for the latest result.
        #[arg(long)]
        attempt: Option<String>,
    },
    /// Show worker health and reserved/total CPU, RAM and GPU resources.
    Workers,
    Stats,
    Hardware,
    Attempts {
        id: String,
    },
    Resolve {
        id: String,
        #[arg(long, required = true)]
        assume_stopped: bool,
    },
    Status {
        id: String,
    },
    Cancel {
        id: String,
    },
    Logs {
        id: String,
        #[arg(long)]
        follow: bool,
    },
}

fn parse_mem(s: &str) -> Result<u64, String> {
    let s = s.to_ascii_uppercase();
    let (digits, mult) = if let Some(n) = s.strip_suffix('G') {
        (n, 1024u64)
    } else if let Some(n) = s.strip_suffix('M') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .filter(|n| *n > 0)
        .ok_or_else(|| "use positive MiB, e.g. 256M or 2G".into())
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let code = match run().await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("runspk: {e:#}");
            if e.downcast_ref::<reqwest::Error>()
                .is_some_and(|e| e.is_connect() || e.is_timeout())
            {
                eprintln!(
                    "Run runspk doctor. Start the scheduler with runspk start, or check the configured URL and SSH tunnel."
                );
            } else if e
                .downcast_ref::<runspk::client::HttpError>()
                .is_some_and(|e| e.status == reqwest::StatusCode::UNAUTHORIZED)
            {
                eprintln!(
                    "Check the private connection profile or RUNSPK_TOKEN; a worker invitation must come from this scheduler."
                );
            }
            1
        }
    };
    std::process::exit(code);
}
async fn run() -> anyhow::Result<i32> {
    let a = Args::parse();
    let human = a.human || (!a.json && std::io::stdout().is_terminal());
    let kind = match &a.command {
        Action::List(_) => "list",
        Action::Queue(_) => "queue",
        Action::Workers => "workers",
        Action::Status { .. } => "status",
        Action::Cancel { .. } => "cancel",
        Action::Resolve { .. } => "resolve",
        Action::Explain { .. } => "explain",
        Action::Stats => "stats",
        Action::Fetch { .. } => "fetch",
        Action::Attempts { .. } => "attempts",
        _ => "other",
    };
    if let Action::Completions { shell } = &a.command {
        clap_complete::generate(
            *shell,
            &mut Args::command(),
            "runspk",
            &mut std::io::stdout(),
        );
        return Ok(0);
    }
    if matches!(&a.command,Action::Submit(spec) | Action::Run{spec,..} if spec.dry_run) {
        let spec = match a.command {
            Action::Submit(spec) | Action::Run { spec, .. } => spec,
            _ => unreachable!(),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&spec.prepare().await?.manifest()?)?
        );
        return Ok(0);
    }
    if matches!(a.command, Action::Hardware) {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"physical":runspk::runner::physical_resources()?,"gpus":runspk::hardware::discover_gpus()?})
            )?
        );
        return Ok(0);
    }
    if let Action::Backup { db, out } = a.command {
        let displayed = out.clone();
        tokio::task::spawn_blocking(move || runspk::store::backup_database(&db, &out)).await??;
        println!("{}", serde_json::json!({"backup":displayed}));
        return Ok(0);
    }
    let config_path = profile::config_path(a.config)?;
    if let Action::Init { state_dir } = a.command {
        let value = setup::init(&config_path, a.url, state_dir)?;
        if a.json {
            println!("{}", serde_json::to_string(&value)?);
        } else {
            println!(
                "{} {}\nNext: runspk start\nNo services, shell files or network settings were changed.",
                if value["created"] == true {
                    "Initialized"
                } else {
                    "Already initialized:"
                },
                config_path.display()
            );
        }
        return Ok(0);
    }
    if let Action::Join(args) = a.command {
        setup::join(&config_path, args)?;
        return Ok(0);
    }
    if matches!(a.command, Action::Doctor) {
        let (ok, value) = setup::doctor(&config_path, a.url, a.token).await?;
        if human {
            display::render("doctor", &value)?;
        } else {
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        return Ok(if ok { 0 } else { 1 });
    }
    let saved = Profile::load(&config_path)?;
    let url = a
        .url
        .or_else(|| saved.as_ref().map(|p| p.url.clone()))
        .unwrap_or_else(|| "http://127.0.0.1:8765".into());
    let token = a
        .token
        .or_else(|| saved.as_ref().map(|p| p.token.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!("not initialized; run runspk init, or provide RUNSPK_TOKEN / --token")
        })?;
    let connection = Profile {
        url: url.clone(),
        token: token.clone(),
        state_dir: saved.and_then(|p| p.state_dir),
    };
    if let Action::Start(args) = a.command {
        setup::start(&connection, args)?;
        return Ok(0);
    }
    if let Action::Invite { out } = a.command {
        Profile {
            url,
            token,
            state_dir: None,
        }
        .save(&out)?;
        if a.json {
            println!(
                "{}",
                serde_json::json!({"invitation":out,"administrative_access":true})
            );
        } else {
            println!(
            "Invitation saved privately to {}. Copy it over SSH; it grants full scheduler access.
On the worker: runspk join {} --id NAME --cpu 1 --mem 256M --allow-native",
            out.display(),
            out.display()
        );
        }
        return Ok(0);
    }
    let c = Client::new(url, token)?;
    let value = match a.command {
        Action::Init { .. }
        | Action::Join(_)
        | Action::Doctor
        | Action::Start(_)
        | Action::Invite { .. }
        | Action::Completions { .. } => unreachable!(),
        Action::Submit(args) => {
            let j = submit(&c, args).await?;
            if !a.json {
                println!("{}", j.id);
                return Ok(0);
            }
            serde_json::to_value(j)?
        }
        Action::Run { spec, wait_timeout } => {
            let j = submit(&c, spec).await?;
            if a.json {
                println!("{}", serde_json::json!({"event":"submitted","job":j}));
            } else {
                eprintln!("runspk: submitted {}", j.id);
            }
            return wait_for(&c, &j.id, true, a.json, human, wait_timeout).await;
        }
        Action::Wait { id, timeout } => {
            return wait_for(&c, &id, false, a.json, human, timeout).await;
        }
        Action::List(filter) => {
            serde_json::to_value(c.get_query::<Vec<Job>>("/jobs", &filter).await?)?
        }
        Action::Queue(mut filter) => {
            anyhow::ensure!(
                filter.state.is_none_or(|s| s == JobState::Queued),
                "queue only accepts --state queued"
            );
            filter.state = Some(JobState::Queued);
            serde_json::to_value(c.get_query::<Vec<Job>>("/jobs", &filter).await?)?
        }
        Action::Explain { id } => {
            c.get::<serde_json::Value>(&format!("/jobs/{id}/explain"))
                .await?
        }
        Action::Prune {
            before,
            limit,
            dry_run,
        } => {
            c.post::<serde_json::Value>(
                "/history/prune",
                &PruneRequest {
                    before_ms: before,
                    limit,
                    dry_run,
                },
            )
            .await?
        }
        Action::Backup { .. } => unreachable!(),
        Action::Attempts { id } => serde_json::to_value(
            c.get::<Vec<Attempt>>(&format!("/jobs/{id}/attempts"))
                .await?,
        )?,
        Action::Resolve { id, assume_stopped } => serde_json::to_value(
            c.post::<Job>(&format!("/jobs/{id}/resolve"), &assume_stopped)
                .await?,
        )?,
        Action::Fetch { id, into, attempt } => {
            anyhow::ensure!(
                attempt
                    .as_ref()
                    .is_none_or(|attempt| uuid::Uuid::parse_str(attempt).is_ok()),
                "--attempt needs an attempt UUID; run runspk attempts {id}, or omit --attempt for the latest result"
            );
            let query = attempt
                .map(|attempt| format!("?attempt={attempt}"))
                .unwrap_or_default();
            let artifacts: runspk::workspace::Artifacts = c
                .get_with_timeout(
                    &format!("/jobs/{id}/artifacts{query}"),
                    runspk::client::TRANSFER_TIMEOUT,
                )
                .await?;
            let target = into.clone();
            let attempt_id = artifacts.attempt_id.clone();
            tokio::task::spawn_blocking(move || {
                runspk::workspace::materialize(&artifacts.bundle, &target)
            })
            .await??;
            serde_json::json!({"job_id":id,"attempt_id":attempt_id,"directory":into})
        }
        Action::Workers => serde_json::to_value(c.get::<Vec<Worker>>("/workers").await?)?,
        Action::Stats => c.get::<serde_json::Value>("/stats").await?,
        Action::Hardware => unreachable!(),
        Action::Status { id } => serde_json::to_value(c.get::<Job>(&format!("/jobs/{id}")).await?)?,
        Action::Cancel { id } => {
            serde_json::to_value(c.post::<Job>(&format!("/jobs/{id}/cancel"), &()).await?)?
        }
        Action::Logs { id, follow } => {
            let mut lengths = (0, 0);
            let mut attempt = None;
            loop {
                let terminal = c.get::<Job>(&format!("/jobs/{id}")).await?.state.terminal();
                let logs: Logs = c.get(&format!("/jobs/{id}/logs")).await?;
                if logs.attempt_id != attempt {
                    lengths = (0, 0);
                    attempt = logs.attempt_id.clone();
                }
                if a.json {
                    println!("{}", serde_json::to_string(&logs)?);
                } else {
                    std::io::stdout()
                        .write_all(&logs.stdout.as_bytes()[lengths.0.min(logs.stdout.len())..])?;
                    std::io::stderr()
                        .write_all(&logs.stderr.as_bytes()[lengths.1.min(logs.stderr.len())..])?;
                    std::io::stdout().flush()?;
                }
                lengths = (logs.stdout.len(), logs.stderr.len());
                if !follow || terminal {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            return Ok(0);
        }
    };
    if human {
        display::render(kind, &value)?;
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(0)
}

async fn wait_for(
    client: &Client,
    id: &str,
    stream: bool,
    json: bool,
    human: bool,
    timeout: Option<u64>,
) -> anyhow::Result<i32> {
    tokio::select! {
        result = follow_job(client,id,stream,json,human) => result,
        _ = runspk::shutdown_signal() => {
            eprintln!("runspk: interrupted; job {id} remains submitted");
            Ok(130)
        }
        _ = async {
            match timeout {
                Some(secs) => tokio::time::sleep(std::time::Duration::from_secs(secs)).await,
                None => std::future::pending::<()>().await,
            }
        } => {
            eprintln!("runspk: wait timed out; job {id} remains submitted");
            Ok(124)
        }
    }
}

async fn follow_job(
    client: &Client,
    id: &str,
    stream: bool,
    json: bool,
    human: bool,
) -> anyhow::Result<i32> {
    let mut lengths = (0, 0);
    let mut attempt = None;
    loop {
        let job: Job = wait_read(client, &format!("/jobs/{id}")).await?;
        if stream {
            let logs: Logs = wait_read(client, &format!("/jobs/{id}/logs")).await?;
            if logs.attempt_id != attempt {
                lengths = (0, 0);
                attempt = logs.attempt_id.clone();
            }
            if json {
                if lengths != (logs.stdout.len(), logs.stderr.len()) {
                    println!("{}", serde_json::json!({"event":"logs","logs":logs}));
                }
            } else {
                std::io::stdout()
                    .write_all(&logs.stdout.as_bytes()[lengths.0.min(logs.stdout.len())..])?;
                std::io::stderr()
                    .write_all(&logs.stderr.as_bytes()[lengths.1.min(logs.stderr.len())..])?;
                std::io::stdout().flush()?;
                std::io::stderr().flush()?;
            }
            lengths = (logs.stdout.len(), logs.stderr.len());
        }
        if job.state.terminal() {
            if json && stream {
                println!("{}", serde_json::json!({"event":"finished","job":job}));
            } else if !stream {
                if human {
                    display::render("status", &serde_json::to_value(&job)?)?;
                } else {
                    println!("{}", serde_json::to_string_pretty(&job)?);
                }
            } else if let Some(error) = &job.error {
                eprintln!("runspk: {error}; inspect with runspk status {id}");
            }
            return Ok(match job.state {
                JobState::Succeeded => 0,
                JobState::Cancelled => 130,
                _ => job
                    .exit_code
                    .filter(|code| (1..=255).contains(code))
                    .unwrap_or(1),
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

async fn wait_read<T: serde::de::DeserializeOwned>(
    client: &Client,
    path: &str,
) -> anyhow::Result<T> {
    loop {
        match client.get(path).await {
            Ok(value) => return Ok(value),
            Err(error)
                if error
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(|e| e.is_connect() || e.is_timeout()) =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(error) => return Err(error),
        }
    }
}
