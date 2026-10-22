//! Interactive presentation of scheduler API snapshots; never makes placement decisions.
use anyhow::{Context, Result, ensure};
use crossterm::{
    cursor::Show,
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Clear, Paragraph, Row, Table, TableState},
};
use runspk::{
    client::Client,
    model::*,
    store::{LogChunk, LogQuery},
};
use serde_json::Value;
use std::{
    io::{self, IsTerminal},
    panic::PanicHookInfo,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task::{AbortHandle, JoinSet};

const PAGE_SIZE: u32 = 200;
const WORKER_LIMIT: usize = 512;
const TEXT_LIMIT: usize = 64 * 1024;
const LINE_LIMIT: usize = 2000;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const STATES: [JobState; 8] = [
    JobState::Queued,
    JobState::Assigned,
    JobState::Running,
    JobState::Cancelling,
    JobState::Uncertain,
    JobState::Succeeded,
    JobState::Failed,
    JobState::Cancelled,
];
type PanicHook = dyn Fn(&PanicHookInfo<'_>) + Send + Sync + 'static;

fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
}

struct TerminalGuard(Option<Arc<PanicHook>>);

impl TerminalGuard {
    fn enter() -> Result<Self> {
        let previous: Arc<PanicHook> = std::panic::take_hook().into();
        let hook = previous.clone();
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal();
            hook(info);
        }));
        let guard = Self(Some(previous));
        enable_raw_mode().context("enable monitor terminal input")?;
        execute!(io::stdout(), EnterAlternateScreen).context("open monitor screen")?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
        if !std::thread::panicking()
            && let Some(previous) = self.0.take()
        {
            std::panic::set_hook(Box::new(move |info| previous(info)));
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum View {
    Jobs,
    Workers,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pane {
    Details,
    Attempts,
    Explain,
    Stdout,
    Stderr,
}

impl Pane {
    fn next(self) -> Self {
        match self {
            Self::Details => Self::Attempts,
            Self::Attempts => Self::Explain,
            Self::Explain => Self::Stdout,
            Self::Stdout => Self::Stderr,
            Self::Stderr => Self::Details,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Details => "Details",
            Self::Attempts => "Attempts",
            Self::Explain => "Explain",
            Self::Stdout => "Stdout",
            Self::Stderr => "Stderr",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sort {
    Newest,
    State,
    Worker,
    Cpu,
    Memory,
}

impl Sort {
    fn next(self) -> Self {
        match self {
            Self::Newest => Self::State,
            Self::State => Self::Worker,
            Self::Worker => Self::Cpu,
            Self::Cpu => Self::Memory,
            Self::Memory => Self::Newest,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DetailKey {
    id: String,
    pane: Pane,
}

struct Detail {
    key: DetailKey,
    text: String,
    error: Option<String>,
    updated: Option<Instant>,
}

enum Update {
    Overview {
        revision: u64,
        jobs: Result<Vec<Job>>,
        workers: Result<Vec<Worker>>,
        stats: Result<Value>,
    },
    Detail {
        key: DetailKey,
        job: Result<Job>,
        text: Result<String>,
    },
    Cancel {
        id: String,
        result: Result<Job>,
    },
}

struct App {
    jobs: Vec<Job>,
    workers: Vec<Worker>,
    stats: Value,
    visible_jobs: Vec<usize>,
    visible_workers: Vec<usize>,
    job_selection: TableState,
    worker_selection: TableState,
    view: View,
    pane: Pane,
    sort: Sort,
    reverse: bool,
    filter: JobFilter,
    page_history: Vec<i64>,
    revision: u64,
    search: String,
    editing_search: bool,
    saved_search: String,
    paused: bool,
    help: bool,
    help_scroll: u16,
    confirmation: Option<String>,
    detail: Option<Detail>,
    scroll: u16,
    horizontal_scroll: u16,
    follow: bool,
    errors: Vec<String>,
    notice: String,
    overview_updated: Option<Instant>,
    workers_truncated: bool,
    force_overview: bool,
    force_detail: bool,
}

impl App {
    fn new() -> Self {
        Self {
            jobs: Vec::new(),
            workers: Vec::new(),
            stats: Value::Null,
            visible_jobs: Vec::new(),
            visible_workers: Vec::new(),
            job_selection: TableState::default(),
            worker_selection: TableState::default(),
            view: View::Jobs,
            pane: Pane::Details,
            sort: Sort::Newest,
            reverse: false,
            filter: JobFilter {
                limit: Some(PAGE_SIZE),
                ..Default::default()
            },
            page_history: Vec::new(),
            revision: 0,
            search: String::new(),
            editing_search: false,
            saved_search: String::new(),
            paused: false,
            help: false,
            help_scroll: 0,
            confirmation: None,
            detail: None,
            scroll: 0,
            horizontal_scroll: 0,
            follow: true,
            errors: Vec::new(),
            notice: String::new(),
            overview_updated: None,
            workers_truncated: false,
            force_overview: true,
            force_detail: false,
        }
    }

    fn selected_job(&self) -> Option<&Job> {
        self.job_selection
            .selected()
            .and_then(|i| self.visible_jobs.get(i))
            .and_then(|&i| self.jobs.get(i))
    }

    fn selected_worker(&self) -> Option<&Worker> {
        self.worker_selection
            .selected()
            .and_then(|i| self.visible_workers.get(i))
            .and_then(|&i| self.workers.get(i))
    }

    fn detail_key(&self) -> Option<DetailKey> {
        (self.view == View::Jobs)
            .then(|| self.selected_job())
            .flatten()
            .map(|job| DetailKey {
                id: job.id.clone(),
                pane: self.pane,
            })
    }

    fn rebuild(&mut self) {
        let job_id = self.selected_job().map(|job| job.id.clone());
        let worker_id = self
            .selected_worker()
            .map(|w| w.registration.auth.id.clone());
        self.rebuild_preserving(job_id, worker_id);
    }

    fn rebuild_preserving(&mut self, job_id: Option<String>, worker_id: Option<String>) {
        let query = self.search.to_lowercase();
        self.visible_jobs = self
            .jobs
            .iter()
            .enumerate()
            .filter_map(|(i, job)| {
                let text = format!(
                    "{} {} {} {} {} {}",
                    job.id,
                    state_name(job.state),
                    job.worker_id.as_deref().unwrap_or(""),
                    job.spec.command.join(" "),
                    job.spec.name.as_deref().unwrap_or(""),
                    job.spec.tags.join(" ")
                );
                text.to_lowercase().contains(&query).then_some(i)
            })
            .collect();
        self.visible_jobs.sort_by(|&a, &b| {
            let (a, b) = (&self.jobs[a], &self.jobs[b]);
            let ordering = match self.sort {
                Sort::Newest => b.seq.cmp(&a.seq),
                Sort::State => state_name(a.state).cmp(state_name(b.state)),
                Sort::Worker => a.worker_id.cmp(&b.worker_id),
                Sort::Cpu => b.spec.resources.cpu.cmp(&a.spec.resources.cpu),
                Sort::Memory => b.spec.resources.mem_mib.cmp(&a.spec.resources.mem_mib),
            }
            .then_with(|| b.seq.cmp(&a.seq));
            if self.reverse {
                ordering.reverse()
            } else {
                ordering
            }
        });
        self.visible_workers = self
            .workers
            .iter()
            .enumerate()
            .filter_map(|(i, worker)| {
                let text = format!("{} {:?}", worker.registration.auth.id, worker.state);
                text.to_lowercase().contains(&query).then_some(i)
            })
            .collect();
        self.visible_workers.sort_by(|&a, &b| {
            let (a, b) = (&self.workers[a], &self.workers[b]);
            let ordering = match self.sort {
                Sort::State => worker_state(a.state).cmp(worker_state(b.state)),
                Sort::Cpu => b.reserved.cpu.cmp(&a.reserved.cpu),
                Sort::Memory => b.reserved.mem_mib.cmp(&a.reserved.mem_mib),
                _ => a.registration.auth.id.cmp(&b.registration.auth.id),
            }
            .then_with(|| a.registration.auth.id.cmp(&b.registration.auth.id));
            if self.reverse {
                ordering.reverse()
            } else {
                ordering
            }
        });
        let selected = job_id.and_then(|id| {
            self.visible_jobs
                .iter()
                .position(|&i| self.jobs[i].id == id)
        });
        preserve_selection(&mut self.job_selection, self.visible_jobs.len(), selected);
        let selected = worker_id.and_then(|id| {
            self.visible_workers
                .iter()
                .position(|&i| self.workers[i].registration.auth.id == id)
        });
        preserve_selection(
            &mut self.worker_selection,
            self.visible_workers.len(),
            selected,
        );
    }

    fn query_changed(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        self.jobs.clear();
        self.visible_jobs.clear();
        self.job_selection.select(None);
        self.detail = None;
        self.force_overview = true;
        self.scroll = 0;
    }

    // Returns false only when the user explicitly exits.
    fn key(&mut self, key: KeyEvent) -> bool {
        if key.kind == KeyEventKind::Release {
            return true;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c' | 'd'))
        {
            return false;
        }
        if self.confirmation.is_some() {
            return true;
        }
        if self.editing_search {
            match key.code {
                KeyCode::Esc => {
                    self.search = self.saved_search.clone();
                    self.editing_search = false;
                }
                KeyCode::Enter => self.editing_search = false,
                KeyCode::Backspace => {
                    self.search.pop();
                }
                KeyCode::Char(c) if !c.is_control() && self.search.len() < 256 => {
                    self.search.push(c)
                }
                _ => {}
            }
            self.scroll = 0;
            self.rebuild();
            return true;
        }
        if self.help {
            match key.code {
                KeyCode::Esc | KeyCode::Char('?' | 'q') => self.help = false,
                KeyCode::Down | KeyCode::Char('j') => {
                    self.help_scroll = self.help_scroll.saturating_add(1)
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.help_scroll = self.help_scroll.saturating_sub(1)
                }
                KeyCode::PageDown => self.help_scroll = self.help_scroll.saturating_add(8),
                KeyCode::PageUp => self.help_scroll = self.help_scroll.saturating_sub(8),
                KeyCode::Home => self.help_scroll = 0,
                KeyCode::End => self.help_scroll = HELP.lines().count() as u16,
                _ => {}
            }
            return true;
        }
        match key.code {
            KeyCode::Char('q') => return false,
            KeyCode::Char('?') => self.help = true,
            KeyCode::Char('1') => {
                self.view = View::Jobs;
                self.scroll = 0;
            }
            KeyCode::Char('2') => {
                self.view = View::Workers;
                self.scroll = 0;
            }
            KeyCode::Char('/') => {
                self.saved_search = self.search.clone();
                self.editing_search = true;
            }
            KeyCode::Esc => {
                self.search.clear();
                self.rebuild();
            }
            KeyCode::Char(' ') => self.paused = !self.paused,
            KeyCode::Char('r') => {
                self.force_overview = true;
                self.force_detail = true;
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Home | KeyCode::Char('g') => self.move_selection(isize::MIN),
            KeyCode::End | KeyCode::Char('G') => self.move_selection(isize::MAX),
            KeyCode::PageUp => {
                self.follow = false;
                self.scroll = self.scroll.saturating_sub(10);
            }
            KeyCode::PageDown => {
                self.follow = false;
                self.scroll = self.scroll.saturating_add(10);
            }
            KeyCode::Char(']') => self.horizontal_scroll = self.horizontal_scroll.saturating_add(8),
            KeyCode::Char('[') => self.horizontal_scroll = self.horizontal_scroll.saturating_sub(8),
            KeyCode::Char('t') => {
                self.follow = !self.follow;
            }
            KeyCode::Tab | KeyCode::Right => {
                self.pane = self.pane.next();
                self.scroll = 0;
                self.follow = true;
            }
            KeyCode::Char('d') | KeyCode::Enter => {
                self.pane = Pane::Details;
                self.scroll = 0;
            }
            KeyCode::Char('a') => {
                self.pane = Pane::Attempts;
                self.scroll = 0;
            }
            KeyCode::Char('e') => {
                self.pane = Pane::Explain;
                self.scroll = 0;
            }
            KeyCode::Char('o') => {
                self.pane = Pane::Stdout;
                self.scroll = 0;
                self.follow = true;
            }
            KeyCode::Char('i') => {
                self.pane = Pane::Stderr;
                self.scroll = 0;
                self.follow = true;
            }
            KeyCode::Char('s') => {
                self.sort = self.sort.next();
                self.rebuild();
            }
            KeyCode::Char('S') => {
                self.reverse = !self.reverse;
                self.rebuild();
            }
            KeyCode::Char('f') => {
                self.filter.state = match self.filter.state {
                    None => Some(STATES[0]),
                    Some(state) => STATES
                        .iter()
                        .position(|&s| s == state)
                        .and_then(|i| STATES.get(i + 1).copied()),
                };
                self.filter.after = 0;
                self.page_history.clear();
                self.query_changed();
            }
            KeyCode::Char('w') => {
                self.filter.worker = if self.filter.worker.is_some() {
                    None
                } else {
                    match self.view {
                        View::Workers => self
                            .selected_worker()
                            .map(|w| w.registration.auth.id.clone()),
                        View::Jobs => self.selected_job().and_then(|job| job.worker_id.clone()),
                    }
                };
                self.view = View::Jobs;
                self.search.clear();
                self.filter.after = 0;
                self.page_history.clear();
                self.query_changed();
            }
            KeyCode::Char('n') if self.jobs.len() == PAGE_SIZE as usize => {
                if let Some(after) = self.jobs.iter().map(|job| job.seq).max() {
                    self.page_history.push(self.filter.after);
                    self.filter.after = after;
                    self.query_changed();
                }
            }
            KeyCode::Char('b') => {
                if let Some(after) = self.page_history.pop() {
                    self.filter.after = after;
                    self.query_changed();
                }
            }
            KeyCode::Char('c') if self.view == View::Jobs && key.kind == KeyEventKind::Press => {
                if let Some(job) = self.selected_job() {
                    if job.state.terminal() {
                        self.notice = "Selected job is already terminal.".into();
                    } else {
                        self.confirmation = Some(job.id.clone());
                    }
                }
            }
            _ => {}
        }
        true
    }

    fn move_selection(&mut self, delta: isize) {
        let (selection, len) = match self.view {
            View::Jobs => (&mut self.job_selection, self.visible_jobs.len()),
            View::Workers => (&mut self.worker_selection, self.visible_workers.len()),
        };
        if len == 0 {
            selection.select(None);
            return;
        }
        let next = selection
            .selected()
            .unwrap_or(0)
            .saturating_add_signed(delta)
            .min(len - 1);
        selection.select(Some(next));
        self.scroll = 0;
        self.horizontal_scroll = 0;
    }

    fn apply(&mut self, update: Update) {
        match update {
            Update::Overview {
                revision,
                jobs,
                workers,
                stats,
            } if revision == self.revision => {
                let job_id = self.selected_job().map(|job| job.id.clone());
                let worker_id = self
                    .selected_worker()
                    .map(|w| w.registration.auth.id.clone());
                self.errors.clear();
                let mut fresh = false;
                match jobs {
                    Ok(jobs) => {
                        self.jobs = jobs;
                        fresh = true;
                    }
                    Err(e) => self.errors.push(format!("jobs: {e}")),
                }
                match workers {
                    Ok(mut workers) => {
                        self.workers_truncated = workers.len() > WORKER_LIMIT;
                        workers.truncate(WORKER_LIMIT);
                        self.workers = workers;
                    }
                    Err(e) => self.errors.push(format!("workers: {e}")),
                }
                match stats {
                    Ok(stats) => self.stats = stats,
                    Err(e) => self.errors.push(format!("stats: {e}")),
                }
                if fresh {
                    self.overview_updated = Some(Instant::now());
                }
                self.rebuild_preserving(job_id, worker_id);
            }
            Update::Detail { key, job, text } if self.detail_key().as_ref() == Some(&key) => {
                let mut errors = Vec::new();
                match job {
                    Ok(job) => {
                        if let Some(current) = self.jobs.iter_mut().find(|j| j.id == job.id) {
                            *current = job;
                        }
                    }
                    Err(e) => errors.push(format!("job: {e}")),
                }
                if let Some(detail) = self.detail.as_mut().filter(|detail| detail.key == key) {
                    match text {
                        Ok(text) => {
                            detail.text = text;
                            detail.updated = Some(Instant::now());
                        }
                        Err(e) => {
                            if detail.updated.is_none() {
                                detail.text = safe(&format!(
                                    "Unable to load {}.\n{e}\n\nr: retry   [ / ]: scroll long error text",
                                    key.pane.name()
                                ));
                            }
                            errors.push(format!("{}: {e}", key.pane.name()));
                        }
                    }
                    detail.error = (!errors.is_empty()).then(|| errors.join("; "));
                }
            }
            Update::Cancel { id, result } => {
                self.notice = match result {
                    Ok(job) => {
                        let state = state_name(job.state);
                        if let Some(current) = self.jobs.iter_mut().find(|j| j.id == id) {
                            *current = job;
                        }
                        format!("Cancellation accepted for {id}: {state}")
                    }
                    Err(e) => format!(
                        "Cancellation for {id}: {e}. Outcome may be unknown; refresh before retrying."
                    ),
                };
                self.force_overview = true;
                self.force_detail = true;
            }
            _ => {}
        }
    }
}

fn preserve_selection(selection: &mut TableState, len: usize, preferred: Option<usize>) {
    selection.select(if len == 0 {
        None
    } else {
        Some(
            preferred
                .unwrap_or(selection.selected().unwrap_or(0))
                .min(len - 1),
        )
    });
}

fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Assigned => "assigned",
        JobState::Running => "running",
        JobState::Cancelling => "cancelling",
        JobState::Uncertain => "uncertain",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
    }
}

fn worker_state(state: WorkerState) -> &'static str {
    match state {
        WorkerState::Online => "online",
        WorkerState::Offline => "offline",
        WorkerState::Draining => "draining",
    }
}

// All API strings are untrusted, including errors, IDs and command arguments.
fn safe(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c == '\t' {
                ' '
            } else if (c.is_control() && c != '\n')
                || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
            {
                '�'
            } else {
                c
            }
        })
        .collect()
}

fn cell(text: impl AsRef<str>) -> String {
    safe(text.as_ref()).replace('\n', " ")
}

fn log_tail(text: &str) -> String {
    let mut start = text.len().saturating_sub(TEXT_LIMIT);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    let tail = &text[start..];
    let line_start = tail
        .match_indices('\n')
        .rev()
        .nth(LINE_LIMIT)
        .map_or(0, |(i, _)| i + 1);
    let clipped = start > 0 || line_start > 0;
    format!(
        "{}{}",
        if clipped {
            "[Earlier output omitted; use runspk logs for the API snapshot]\n"
        } else {
            ""
        },
        safe(&tail[line_start..])
    )
}

fn job_text(job: &Job) -> String {
    let end = job.finished_ms.unwrap_or_else(now_ms);
    let queued = duration(job.submitted_ms, job.started_ms.unwrap_or(end));
    let elapsed = job
        .started_ms
        .map_or("not started".into(), |start| duration(start, end));
    safe(&format!(
        "Job: {}\nName: {}\nTags: {}\nRequired labels: {:?}\nState: {}\nCommand: {}\nWorker: {}\nRequested: {} CPU / {} MiB / {} GPU\nResources are reservations, not measured usage.\nPriority: {}   Attempts: {} / {} retries\nQueue wait / prior retry history: {}\nLatest attempt elapsed: {}\nSubmitted: {} ms since epoch\nStarted: {:?}   Finished: {:?}\nExit: {:?}\nReason: {}\nAttempt: {}\nAllocated GPU IDs: {}\nRetry at: {}   Lease until: {}\nTimeout: {}s\nWorking directory: {}\nImage: {}\nWorkspace: {}\nOutputs: {}",
        job.id,
        job.spec.name.as_deref().unwrap_or("—"),
        job.spec.tags.join(", "),
        job.spec.required_labels,
        state_name(job.state),
        job.spec.command.join(" "),
        job.worker_id.as_deref().unwrap_or("unassigned"),
        job.spec.resources.cpu,
        job.spec.resources.mem_mib,
        job.spec.gpu,
        job.spec.priority,
        job.attempts,
        job.spec.max_retries,
        queued,
        elapsed,
        job.submitted_ms,
        job.started_ms,
        job.finished_ms,
        job.exit_code,
        job.error.as_deref().unwrap_or("—"),
        job.attempt_id.as_deref().unwrap_or("—"),
        job.allocated_gpus.join(", "),
        job.retry_at_ms,
        job.lease_until_ms,
        job.spec.timeout_secs,
        job.spec.cwd.as_deref().unwrap_or("worker default"),
        job.spec.image.as_deref().unwrap_or("native"),
        job.spec.workspace_id.as_deref().unwrap_or("—"),
        job.spec.outputs.join(", "),
    ))
}

fn duration(start: i64, end: i64) -> String {
    let seconds = end.saturating_sub(start).max(0) / 1000;
    format!(
        "{}h {}m {}s",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )
}

fn worker_text(worker: &Worker) -> String {
    let r = &worker.registration;
    let mut text = format!(
        "Worker: {}\nLabels: {:?}\nHealth: {}\nLast heartbeat: {}s ago\nReserved / capacity:\n  CPU: {} / {}\n  RAM: {} / {} MiB\n  GPU: {} / {}\nPhysical inventory: {} CPU / {} MiB\nReservations are not measured CPU/RAM usage.\nNative: {}   Container runtime: {:?}\nProject staging: {}\nReserved GPU IDs: {}\n\nGPU telemetry (registration snapshot; age unknown):\n",
        r.auth.id,
        r.labels,
        worker_state(worker.state),
        now_ms().saturating_sub(worker.last_seen_ms).max(0) / 1000,
        worker.reserved.cpu,
        r.capacity.cpu,
        worker.reserved.mem_mib,
        r.capacity.mem_mib,
        worker.reserved_gpus.len(),
        r.gpus.len(),
        r.physical.cpu,
        r.physical.mem_mib,
        r.allow_native,
        r.runtime,
        r.project_staging,
        worker.reserved_gpus.join(", "),
    );
    if let Some(t) = &worker.telemetry {
        text.push_str(&format!("Measured HOST usage (/proc; entire host, including other applications):\n  CPU busy: {}\n  RAM available: {} / {} MiB (kernel MemAvailable estimate)\n  Received sample {}s ago; worker sample timestamp {}\n\n",t.cpu_busy_percent.map_or("waiting for second sample".into(),|p|format!("{p}%")),t.mem_available_mib,t.mem_total_mib,worker.telemetry_received_ms.map_or(0,|ms|(now_ms()-ms).max(0)/1000),t.sampled_ms));
    } else {
        text.push_str(
            "Measured host CPU/RAM usage: unavailable (legacy worker or sampling failed).\n\n",
        );
    }
    for gpu in &r.gpus {
        text.push_str(&format!(
            "{} · {}\n  {} / {} MiB free; utilization {}%{}\n",
            gpu.id,
            gpu.name,
            gpu.free_memory_mib,
            gpu.memory_mib,
            gpu.utilization_percent,
            if gpu.simulated {
                " (SIMULATED)"
            } else {
                " (measured snapshot)"
            }
        ));
    }
    if r.gpus.is_empty() {
        text.push_str("No GPUs registered.\n");
    }
    safe(&text)
}

async fn detail_update(client: Client, key: DetailKey) -> Update {
    let path = format!("/jobs/{}", key.id);
    let job_request = bounded(client.get::<Job>(&path));
    if key.pane == Pane::Details {
        let job = job_request.await;
        let text = match &job {
            Ok(job) => Ok(job_text(job)),
            Err(e) => Err(anyhow::anyhow!("{e}")),
        };
        return Update::Detail { key, job, text };
    }
    let pane_request = async {
        match key.pane {
            Pane::Attempts => {
                let attempts: Vec<Attempt> =
                    bounded(client.get(&format!("{path}/attempts"))).await?;
                let mut text = String::from("Attempt history (oldest first)\n");
                for a in attempts.iter().rev().take(100).rev() {
                    text.push_str(&format!("\n{}\nWorker: {}   State: {:?}   Exit: {:?}\nAssigned: {}   Started: {:?}   Finished: {:?}\nReason: {}\n",
                        a.id, a.auth.id, a.state, a.exit_code, a.assigned_ms, a.started_ms,
                        a.finished_ms, a.error.as_deref().unwrap_or("—")));
                }
                if attempts.is_empty() {
                    text.push_str("No attempts yet.");
                }
                if attempts.len() > 100 {
                    text.push_str("\nShowing the latest 100 attempts.");
                }
                Ok(safe(&text))
            }
            Pane::Explain => {
                let explanation: Value = bounded(client.get(&format!("{path}/explain"))).await?;
                Ok(safe(&format!(
                    "Scheduler feasibility snapshot; not a dispatch promise.\nResources below are available reservations.\n{}",
                    serde_json::to_string_pretty(&explanation)?
                )))
            }
            Pane::Stdout | Pane::Stderr => {
                let query = LogQuery {
                    tail: Some(TEXT_LIMIT),
                    limit: Some(TEXT_LIMIT),
                    ..Default::default()
                };
                let logs: LogChunk = bounded(client.get_query(&format!("{path}/logs/chunk"), &query))
                    .await.map_err(|error| {
                        if error.downcast_ref::<runspk::client::HttpError>().is_some_and(|e| e.status.as_u16() == 404) {
                            error.context(format!("bounded log endpoint unavailable; upgrade the scheduler or use runspk logs {}", key.id))
                        } else { error }
                    })?;
                let text = if key.pane == Pane::Stdout {
                    &logs.stdout
                } else {
                    &logs.stderr
                };
                Ok(format!(
                    "Latest reported attempt: {}\n{}{}",
                    cell(logs.attempt_id.as_deref().unwrap_or("none")),
                    if logs.stdout_truncated || logs.stderr_truncated {
                        "[Capture may be truncated at the worker limit]\n"
                    } else {
                        ""
                    },
                    if text.is_empty() {
                        "No reported output yet.".into()
                    } else {
                        log_tail(text)
                    }
                ))
            }
            Pane::Details => unreachable!(),
        }
    };
    let (job, text) = tokio::join!(job_request, pane_request);
    Update::Detail { key, job, text }
}

async fn bounded<T>(request: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(REQUEST_TIMEOUT, request)
        .await
        .context("monitor request timed out")?
}

/// Open an interactive monitor. Standard input and output must both be terminals.
pub async fn run(client: Client, refresh_secs: u64) -> Result<()> {
    ensure!(
        io::stdin().is_terminal() && io::stdout().is_terminal(),
        "monitor requires an interactive terminal on stdin and stdout; use list/workers/stats for pipes"
    );
    ensure!(
        refresh_secs > 0,
        "monitor refresh interval must be at least one second"
    );
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let interruption = async move {
        #[cfg(unix)]
        {
            tokio::select! { result = tokio::signal::ctrl_c() => result, _ = sigterm.recv() => Ok(()) }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await
        }
    };
    tokio::pin!(interruption);
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    terminal.clear()?;
    let mut app = App::new();
    let mut tasks = JoinSet::new();
    let mut overview: Option<(u64, AbortHandle)> = None;
    let mut detail: Option<(DetailKey, AbortHandle)> = None;
    let mut cancelling = false;
    let refresh = Duration::from_secs(refresh_secs);
    let mut overview_due = Instant::now();
    let mut detail_due = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut dirty = true;
    let mut last_draw = Instant::now();
    let mut was_paused = false;
    loop {
        let newly_paused = app.paused && !was_paused;
        was_paused = app.paused;
        if overview
            .as_ref()
            .is_some_and(|(revision, _)| *revision != app.revision || newly_paused)
            && let Some((_, handle)) = overview.take()
        {
            handle.abort();
        }
        let key = app.detail_key();
        if detail
            .as_ref()
            .is_some_and(|(old, _)| Some(old) != key.as_ref() || newly_paused)
            && let Some((_, handle)) = detail.take()
        {
            handle.abort();
        }
        if app.detail.as_ref().map(|d| &d.key) != key.as_ref() {
            app.detail = key.clone().map(|key| Detail {
                key,
                text: "Loading…".into(),
                error: None,
                updated: None,
            });
            app.scroll = 0;
            app.horizontal_scroll = 0;
            app.follow = true;
            app.force_detail = key.is_some();
            dirty = true;
        }
        let now = Instant::now();
        if overview.is_none() && (app.force_overview || (!app.paused && now >= overview_due)) {
            let c = client.clone();
            let filter = app.filter.clone();
            let revision = app.revision;
            let handle = tasks.spawn(async move {
                let (jobs, workers, stats) = tokio::join!(
                    bounded(c.get_query::<Vec<Job>>("/jobs", &filter)),
                    bounded(c.get::<Vec<Worker>>("/workers")),
                    bounded(c.get::<Value>("/stats")),
                );
                Update::Overview {
                    revision,
                    jobs,
                    workers,
                    stats,
                }
            });
            overview = Some((revision, handle));
            overview_due = now
                .checked_add(refresh)
                .unwrap_or(now + Duration::from_secs(86400));
            app.force_overview = false;
        }
        if detail.is_none() && (app.force_detail || (!app.paused && now >= detail_due)) {
            if let Some(key) = key {
                let handle = tasks.spawn(detail_update(client.clone(), key.clone()));
                detail = Some((key, handle));
                detail_due = now
                    .checked_add(refresh)
                    .unwrap_or(now + Duration::from_secs(86400));
            }
            app.force_detail = false;
        }
        if dirty || last_draw.elapsed() >= Duration::from_secs(1) {
            terminal.draw(|frame| render(frame, &mut app, refresh_secs, cancelling))?;
            dirty = false;
            last_draw = Instant::now();
        }
        tokio::select! {
            signal = &mut interruption => { signal?; break; }
            update = tasks.join_next_with_id(), if !tasks.is_empty() => {
                match update {
                    Some(Ok((task_id, update))) => {
                        match &update {
                            Update::Overview { .. } => {
                                if overview.as_ref().is_some_and(|(_, handle)| handle.id() == task_id) { overview = None; }
                            }
                            Update::Detail { .. } => {
                                if detail.as_ref().is_some_and(|(_, handle)| handle.id() == task_id) { detail = None; }
                            }
                            Update::Cancel { .. } => cancelling = false,
                        }
                        app.apply(update); dirty = true;
                    }
                    Some(Err(error)) if !error.is_cancelled() => anyhow::bail!("monitor polling task failed: {error}"),
                    _ => {}
                }
            }
            _ = tick.tick() => {
                // Drain a bounded batch so pasted/repeated input cannot starve network results or signals.
                for _ in 0..32 {
                    if !event::poll(Duration::ZERO)? { break; }
                    match event::read()? {
                        Event::Key(key) => {
                            if key.kind == KeyEventKind::Release { continue; }
                            if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c' | 'd')) { return Ok(()); }
                            if let Some(id) = app.confirmation.clone() {
                                if key.kind == KeyEventKind::Press {
                                    match key.code {
                                        KeyCode::Char('y' | 'Y') if !cancelling => {
                                            app.confirmation = None;
                                            cancelling = true;
                                            app.notice = format!("Requesting cancellation of {id}…");
                                            let c = client.clone();
                                            tasks.spawn(async move {
                                                let result = bounded(c.post::<Job>(&format!("/jobs/{id}/cancel"), &())).await;
                                                Update::Cancel { id, result }
                                            });
                                        }
                                        KeyCode::Esc | KeyCode::Char('n' | 'N' | 'q') => app.confirmation = None,
                                        _ => {}
                                    }
                                }
                            } else if !app.key(key) { return Ok(()); }
                            dirty = true;
                        }
                        Event::Resize(_, _) => dirty = true,
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(())
}

fn render(frame: &mut Frame<'_>, app: &mut App, refresh_secs: u64, cancelling: bool) {
    let area = frame.area();
    if area.width < 48 || area.height < 12 {
        frame.render_widget(
            Paragraph::new(
                "runspk monitor\nEnlarge terminal to at least 48 × 12.\nq / Ctrl-C: quit   ?: help",
            )
            .block(Block::default().borders(Borders::ALL)),
            area,
        );
        return;
    }
    let rows = Layout::vertical([
        Constraint::Length(if area.height >= 18 { 5 } else { 3 }),
        Constraint::Min(4),
        Constraint::Length(2),
        Constraint::Length(1),
    ])
    .split(area);
    let age = app.overview_updated.map_or("waiting for API".into(), |t| {
        format!("snapshot {}s old", t.elapsed().as_secs())
    });
    let states = STATES
        .iter()
        .map(|&state| {
            let name = state_name(state);
            format!("{name} {}", app.stats["states"][name].as_u64().unwrap_or(0))
        })
        .collect::<Vec<_>>()
        .join(" · ");
    let reserved_cpu: u64 = app.workers.iter().map(|w| u64::from(w.reserved.cpu)).sum();
    let capacity_cpu: u64 = app
        .workers
        .iter()
        .map(|w| u64::from(w.registration.capacity.cpu))
        .sum();
    let reserved_mem: u128 = app
        .workers
        .iter()
        .map(|w| u128::from(w.reserved.mem_mib))
        .sum();
    let capacity_mem: u128 = app
        .workers
        .iter()
        .map(|w| u128::from(w.registration.capacity.mem_mib))
        .sum();
    let reserved_gpu: usize = app.workers.iter().map(|w| w.reserved_gpus.len()).sum();
    let capacity_gpu: usize = app.workers.iter().map(|w| w.registration.gpus.len()).sum();
    let online = app
        .workers
        .iter()
        .filter(|w| w.state == WorkerState::Online)
        .count();
    let extra = if area.height >= 18 {
        format!(
            "{states}\nWorkers online {online}/{}{} · reserved/capacity: CPU {reserved_cpu}/{capacity_cpu} · MiB {reserved_mem}/{capacity_mem} · GPU {reserved_gpu}/{capacity_gpu}\n",
            app.workers.len(),
            if app.workers_truncated {
                " (first 512)"
            } else {
                ""
            }
        )
    } else {
        String::new()
    };
    let summary = format!(
        "runspk · {} jobs total · policy {} · {} · {} · refresh {}s\n{extra}CPU/RAM/GPU are reservations, not measured usage.",
        app.stats["jobs"]
            .as_u64()
            .map_or("?".into(), |n| n.to_string()),
        app.stats["policy"].as_str().unwrap_or("?"),
        if app.paused { "PAUSED" } else { "LIVE" },
        age,
        refresh_secs
    );
    frame.render_widget(
        Paragraph::new(safe(&summary)).block(Block::default().borders(Borders::BOTTOM)),
        rows[0],
    );
    let direction = if area.width >= 110 {
        Direction::Horizontal
    } else {
        Direction::Vertical
    };
    let body = Layout::default()
        .direction(direction)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[1]);
    match app.view {
        View::Jobs => render_jobs(frame, app, body[0]),
        View::Workers => render_workers(frame, app, body[0]),
    }
    render_detail(frame, app, body[1]);
    let detail_error = app.detail.as_ref().and_then(|d| d.error.as_deref());
    let status = if !app.errors.is_empty() {
        format!(
            "API error (retaining last snapshot): {}",
            app.errors.join("; ")
        )
    } else if let Some(error) = detail_error {
        format!("Detail error (retaining last snapshot): {error}")
    } else if cancelling || !app.notice.is_empty() {
        app.notice.clone()
    } else {
        "Read-only polling; only confirmed cancellation changes a job.".into()
    };
    let search = if app.editing_search {
        format!(
            "Search current page: /{}▏  Enter accept · Esc undo",
            app.search
        )
    } else {
        format!(
            "Search: {}   sort: {:?}{}",
            app.search,
            app.sort,
            if app.reverse { " reversed" } else { "" }
        )
    };
    frame.render_widget(
        Paragraph::new(format!("{}\n{}", cell(status), cell(search))),
        rows[2],
    );
    frame.render_widget(Paragraph::new("1 jobs  2 workers  ↑↓ select  Tab pane  / search  f filter  Space pause  r refresh  c cancel  ? help  q quit").style(Style::default().fg(Color::Cyan)), rows[3]);
    if app.help {
        app.help_scroll = app.help_scroll.min(
            HELP.lines()
                .count()
                .saturating_sub(area.height.min(30).saturating_sub(2) as usize) as u16,
        );
        modal(
            frame,
            "Monitor help · ↑↓ / PgUp/PgDn scroll",
            HELP,
            88,
            30,
            app.help_scroll,
        );
    } else if let Some(id) = &app.confirmation {
        modal(
            frame,
            "Confirm cancellation",
            &format!(
                "Cancel job {}?\n\nThis requests cancellation through the scheduler.\nRunning work may take time to stop.\n\ny: confirm    n / Esc: keep running",
                cell(id)
            ),
            72,
            10,
            0,
        );
    }
}

fn selected_style() -> Style {
    Style::default()
        .bg(Color::DarkGray)
        .add_modifier(Modifier::BOLD)
}

fn render_jobs(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    let title = format!(
        "1 Jobs · {}/{} on page {} · state {} · worker {}",
        app.visible_jobs.len(),
        app.jobs.len(),
        app.page_history.len() + 1,
        app.filter.state.map_or("all", state_name),
        app.filter.worker.as_deref().unwrap_or("all")
    );
    let block = Block::default().borders(Borders::ALL).title(cell(title));
    if app.visible_jobs.is_empty() {
        let text = if app.jobs.is_empty() {
            "No jobs on this page.\nr: refresh · b: previous page · f: state filter"
        } else {
            "No jobs match this page's search.\nEsc: clear search"
        };
        frame.render_widget(Paragraph::new(text).block(block), area);
        return;
    }
    let (columns, widths): (&[usize], Vec<Constraint>) = if area.width >= 100 {
        (
            &[0, 1, 2, 3, 4, 5, 6, 7],
            vec![
                Constraint::Length(9),
                Constraint::Length(14),
                Constraint::Length(10),
                Constraint::Length(4),
                Constraint::Length(7),
                Constraint::Length(3),
                Constraint::Length(12),
                Constraint::Min(8),
            ],
        )
    } else if area.width >= 72 {
        (
            &[0, 1, 2, 3, 4, 7],
            vec![
                Constraint::Length(9),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Length(4),
                Constraint::Length(7),
                Constraint::Min(8),
            ],
        )
    } else if area.width >= 60 {
        (
            &[0, 1, 2, 7],
            vec![
                Constraint::Length(9),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Min(8),
            ],
        )
    } else {
        (
            &[1, 2, 0],
            vec![
                Constraint::Min(12),
                Constraint::Length(10),
                Constraint::Length(9),
            ],
        )
    };
    let rows = app.visible_jobs.iter().map(|&i| {
        let j = &app.jobs[i];
        let values = [
            cell(&j.id),
            cell(j.spec.name.as_deref().unwrap_or("—")),
            state_name(j.state).into(),
            j.spec.resources.cpu.to_string(),
            j.spec.resources.mem_mib.to_string(),
            j.spec.gpu.to_string(),
            cell(j.worker_id.as_deref().unwrap_or("—")),
            cell(j.spec.command.join(" ")),
        ];
        Row::new(
            columns
                .iter()
                .map(|&column| values[column].clone())
                .collect::<Vec<_>>(),
        )
    });
    let headers = [
        "JOB", "NAME", "STATE", "CPU", "MiB", "GPU", "WORKER", "COMMAND",
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(
                columns
                    .iter()
                    .map(|&column| headers[column])
                    .collect::<Vec<_>>(),
            )
            .style(Style::default().fg(Color::Cyan)),
        )
        .block(block)
        .row_highlight_style(selected_style())
        .highlight_symbol("› ")
        .column_spacing(1);
    frame.render_stateful_widget(table, area, &mut app.job_selection);
}

fn render_workers(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    let title = format!(
        "2 Workers · {}{} · reserved / capacity",
        app.visible_workers.len(),
        if app.workers_truncated {
            " (first 512)"
        } else {
            ""
        }
    );
    let block = Block::default().borders(Borders::ALL).title(title);
    if app.visible_workers.is_empty() {
        frame.render_widget(
            Paragraph::new("No workers match.\nRegister a worker or clear the search with Esc.")
                .block(block),
            area,
        );
        return;
    }
    let (columns, widths): (&[usize], Vec<Constraint>) = if area.width >= 72 {
        (
            &[0, 1, 2, 3, 4, 5],
            vec![
                Constraint::Min(12),
                Constraint::Length(8),
                Constraint::Length(9),
                Constraint::Length(17),
                Constraint::Length(7),
                Constraint::Length(8),
            ],
        )
    } else {
        (
            &[0, 1, 2, 4],
            vec![
                Constraint::Min(12),
                Constraint::Length(8),
                Constraint::Length(9),
                Constraint::Length(7),
            ],
        )
    };
    let rows = app.visible_workers.iter().map(|&i| {
        let w = &app.workers[i];
        let r = &w.registration;
        let values = [
            cell(&r.auth.id),
            worker_state(w.state).into(),
            format!("{}/{}", w.reserved.cpu, r.capacity.cpu),
            format!("{}/{}", w.reserved.mem_mib, r.capacity.mem_mib),
            format!("{}/{}", w.reserved_gpus.len(), r.gpus.len()),
            format!("{}s", now_ms().saturating_sub(w.last_seen_ms).max(0) / 1000),
        ];
        Row::new(
            columns
                .iter()
                .map(|&column| values[column].clone())
                .collect::<Vec<_>>(),
        )
    });
    let headers = ["WORKER", "HEALTH", "CPU", "MiB", "GPU", "SEEN"];
    let table = Table::new(rows, widths)
        .header(
            Row::new(
                columns
                    .iter()
                    .map(|&column| headers[column])
                    .collect::<Vec<_>>(),
            )
            .style(Style::default().fg(Color::Cyan)),
        )
        .block(block)
        .row_highlight_style(selected_style())
        .highlight_symbol("› ")
        .column_spacing(1);
    frame.render_stateful_widget(table, area, &mut app.worker_selection);
}

fn render_detail(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    let (title, text) = if app.view == View::Workers {
        (
            "Worker details".into(),
            app.selected_worker()
                .map_or("Select a worker.".into(), worker_text),
        )
    } else if let Some(detail) = &app.detail {
        let age = detail.updated.map_or("loading".into(), |t| {
            format!("{}s old", t.elapsed().as_secs())
        });
        (
            format!(
                "{} · {} · {}{}",
                detail.key.pane.name(),
                detail.key.id,
                age,
                if matches!(detail.key.pane, Pane::Stdout | Pane::Stderr) {
                    " · reported tail ≤64KiB"
                } else {
                    ""
                }
            ),
            detail.text.clone(),
        )
    } else {
        (
            app.pane.name().into(),
            "Select a job. Tab: details / attempts / explain / stdout / stderr".into(),
        )
    };
    let height = area.height.saturating_sub(2) as usize;
    let max_scroll = text
        .lines()
        .count()
        .saturating_sub(height)
        .min(u16::MAX as usize) as u16;
    let logs = app.view == View::Jobs && matches!(app.pane, Pane::Stdout | Pane::Stderr);
    if logs && app.follow {
        app.scroll = max_scroll;
    } else {
        app.scroll = app.scroll.min(max_scroll);
    }
    frame.render_widget(
        Paragraph::new(text)
            .scroll((app.scroll, app.horizontal_scroll))
            .block(Block::default().borders(Borders::ALL).title(cell(title))),
        area,
    );
}

fn modal(frame: &mut Frame<'_>, title: &str, text: &str, width: u16, height: u16, scroll: u16) {
    let full = frame.area();
    let width = width.min(full.width);
    let height = height.min(full.height);
    let area = Rect::new(
        full.x + (full.width - width) / 2,
        full.y + (full.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(safe(text)).scroll((scroll, 0)).block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(Style::default().fg(Color::Yellow)),
        ),
        area,
    );
}

const HELP: &str = "1 / 2       Jobs / workers overview\n↑↓ / j k    Select row; Home/End or g/G: first/last\nTab / →     Cycle job pane\nd / Enter   Job details\na / e       Attempts / scheduler explanation\no / i       Latest reported stdout / stderr\nPgUp/PgDn   Scroll detail pane (stops log following)\n[ / ]       Scroll detail pane horizontally\nt           Toggle following the reported log tail\n/           Search page (ID, name, tags, worker, command)\nEnter/Esc   Accept / undo search; Esc outside search clears it\ns / S       Cycle sort / reverse sort (loaded page only)\nf           Cycle server-side job state filter, including all\nw           Filter jobs by selected worker; again clears it\nn / b       Next / previous job page (200 jobs per API page)\nSpace       Pause periodic polling; keys remain active\nr           Refresh once, including when paused\nc           Cancel selected job after y confirmation\n? / Esc     Open / close help\nq / Ctrl-C  Quit (leaves workloads running)\n\nResources are requested/reserved capacity, not measured usage.\nGPU telemetry is the last registration snapshot; simulated data is labeled.\nLogs are the latest worker-reported attempt, not a direct process stream.\nDisplay retains at most 64KiB / 2000 log lines and 100 attempts.\nOffline APIs retain the last snapshot and show its age; requests time out.\nSIGTERM also exits and restores the terminal.";

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    fn job(id: &str, seq: i64, state: JobState) -> Job {
        serde_json::from_value(serde_json::json!({
            "id": id, "seq": seq, "state": state, "submitted_ms": 10,
            "started_ms": null, "finished_ms": null, "attempt_id": null,
            "exit_code": null, "error": null, "worker_id": "worker-1",
            "spec": { "command": ["echo", id], "resources": {"cpu": 2, "mem_mib": 64},
                "timeout_secs": 60, "trusted": true, "cwd": null }
        }))
        .unwrap()
    }

    fn key(app: &mut App, code: KeyCode) -> bool {
        app.key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    fn screen(app: &mut App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| render(frame, app, 2, false)).unwrap();
        terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect()
    }

    #[test]
    fn selection_survives_sort_refresh_and_empty_search() {
        let mut app = App::new();
        app.jobs = vec![
            job("first", 1, JobState::Queued),
            job("second", 2, JobState::Running),
        ];
        app.rebuild();
        assert_eq!(app.selected_job().unwrap().id, "second");
        key(&mut app, KeyCode::Down);
        key(&mut app, KeyCode::Char('S'));
        assert_eq!(app.selected_job().unwrap().id, "first");
        app.apply(Update::Overview {
            revision: 0,
            jobs: Ok(vec![
                job("second", 2, JobState::Running),
                job("first", 1, JobState::Queued),
            ]),
            workers: Ok(vec![]),
            stats: Ok(Value::Null),
        });
        assert_eq!(app.selected_job().unwrap().id, "first");
        app.search = "missing".into();
        app.rebuild();
        assert!(app.selected_job().is_none());
        key(&mut app, KeyCode::Down);
        assert!(app.selected_job().is_none());
        key(&mut app, KeyCode::Esc);
        assert!(app.selected_job().is_some());
    }

    #[test]
    fn filters_reset_cursor_and_ignore_stale_responses() {
        let mut app = App::new();
        app.filter.after = 200;
        app.page_history.push(0);
        key(&mut app, KeyCode::Char('f'));
        assert_eq!(app.filter.state, Some(JobState::Queued));
        assert_eq!(app.filter.after, 0);
        assert!(app.page_history.is_empty());
        app.apply(Update::Overview {
            revision: 0,
            jobs: Ok(vec![job("stale", 1, JobState::Running)]),
            workers: Ok(vec![]),
            stats: Ok(Value::Null),
        });
        assert!(app.jobs.is_empty());
    }

    #[test]
    fn cancellation_targets_frozen_id_and_requires_confirmation() {
        let mut app = App::new();
        app.jobs = vec![
            job("queued", 1, JobState::Queued),
            job("running", 2, JobState::Running),
        ];
        app.rebuild();
        key(&mut app, KeyCode::Char('c'));
        assert_eq!(app.confirmation.as_deref(), Some("running"));
        key(&mut app, KeyCode::Down);
        assert_eq!(app.selected_job().unwrap().id, "running");
        assert!(screen(&mut app, 100, 30).contains("Confirm cancellation"));
        app.confirmation = None;
        app.jobs = vec![job("done", 3, JobState::Succeeded)];
        app.rebuild();
        key(&mut app, KeyCode::Char('c'));
        assert!(app.confirmation.is_none());
    }

    #[test]
    fn render_handles_resize_empty_help_and_untrusted_strings() {
        let mut app = App::new();
        assert!(screen(&mut app, 20, 4).contains("runspk monitor"));
        assert!(screen(&mut app, 100, 30).contains("No jobs"));
        app.jobs = vec![job("job-1", 1, JobState::Queued)];
        app.jobs[0].spec.command = vec!["echo\u{1b}[2J\u{9b}\u{202e}injected".into()];
        app.rebuild();
        let rendered = screen(&mut app, 160, 40);
        assert!(!rendered.contains('\u{1b}'));
        assert!(!rendered.contains('\u{9b}'));
        assert!(!rendered.contains('\u{202e}'));
        assert!(rendered.contains("reservations, not measured usage"));
        key(&mut app, KeyCode::Char('?'));
        assert!(screen(&mut app, 100, 35).contains("Monitor help"));
    }

    #[test]
    fn failures_keep_snapshot_and_detail_responses_are_scoped_to_selection() {
        let mut app = App::new();
        app.jobs = vec![job("job-1", 1, JobState::Running)];
        app.rebuild();
        app.apply(Update::Overview {
            revision: 0,
            jobs: Err(anyhow::anyhow!("offline")),
            workers: Ok(vec![]),
            stats: Err(anyhow::anyhow!("offline")),
        });
        assert_eq!(app.jobs.len(), 1);
        assert!(screen(&mut app, 160, 30).contains("API error"));
        app.detail = Some(Detail {
            key: app.detail_key().unwrap(),
            text: "old".into(),
            error: None,
            updated: None,
        });
        app.apply(Update::Detail {
            key: DetailKey {
                id: "other".into(),
                pane: Pane::Details,
            },
            job: Err(anyhow::anyhow!("bad")),
            text: Ok("wrong".into()),
        });
        assert_eq!(app.detail.as_ref().unwrap().text, "old");
        app.apply(Update::Detail {
            key: app.detail_key().unwrap(),
            job: Err(anyhow::anyhow!("offline")),
            text: Err(anyhow::anyhow!(
                "unsupported endpoint; upgrade the scheduler"
            )),
        });
        assert!(app.detail.as_ref().unwrap().text.contains("Unable to load"));
        assert!(
            app.detail
                .as_ref()
                .unwrap()
                .text
                .contains("upgrade the scheduler")
        );
    }

    #[test]
    fn log_tail_is_bounded_utf8_safe_and_strips_terminal_controls() {
        let text = format!("{}\u{1b}[31mEND\u{7}", "🐈\n".repeat(40000));
        let tail = log_tail(&text);
        assert!(tail.len() < TEXT_LIMIT + 200);
        assert!(tail.lines().count() <= LINE_LIMIT + 2);
        assert!(tail.contains("END"));
        assert!(!tail.contains('\u{1b}'));
        assert!(!tail.contains('\u{7}'));
    }

    #[test]
    fn overview_shows_names_counts_and_honest_durations() {
        let mut app = App::new();
        let mut job = job("job-1", 1, JobState::Succeeded);
        job.spec.name = Some("my-run".into());
        job.submitted_ms = 1000;
        job.started_ms = Some(4000);
        job.finished_ms = Some(9000);
        let detail = job_text(&job);
        assert!(detail.contains("0h 0m 3s"));
        assert!(detail.contains("Latest attempt elapsed: 0h 0m 5s"));
        assert_eq!(duration(2000, 1000), "0h 0m 0s");
        app.jobs.push(job);
        app.rebuild();
        app.stats =
            serde_json::json!({"jobs":10, "policy":"fifo", "states":{"queued":7,"running":3}});
        let text = screen(&mut app, 220, 35);
        assert!(text.contains("my-run"));
        assert!(text.contains("queued 7"));
        assert!(text.contains("running 3"));
        assert!(text.contains("reserved/capacity: CPU"));
        assert!(screen(&mut app, 48, 12).contains("NAME"));
    }

    #[test]
    fn pause_search_and_navigation_are_local_and_responsive() {
        let mut app = App::new();
        app.force_overview = false;
        key(&mut app, KeyCode::Char(' '));
        assert!(app.paused);
        key(&mut app, KeyCode::Char('/'));
        key(&mut app, KeyCode::Char('q'));
        assert_eq!(app.search, "q");
        key(&mut app, KeyCode::Esc);
        assert!(app.search.is_empty());
        key(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Workers);
        key(&mut app, KeyCode::Char('r'));
        assert!(app.force_overview && app.force_detail);
        assert!(!key(&mut app, KeyCode::Char('q')));
    }

    #[test]
    fn measured_host_telemetry_is_separate_from_reservations() {
        let mut store = runspk::store::Store::open(":memory:").unwrap();
        let registration:Registration=serde_json::from_value(serde_json::json!({"auth":{"id":"measured","session":uuid::Uuid::new_v4().to_string()},"capacity":{"cpu":1,"mem_mib":64},"physical":{"cpu":4,"mem_mib":4096},"allow_native":true})).unwrap();
        let mut worker = store.register(registration).unwrap();
        worker.telemetry = Some(runspk::telemetry::HostTelemetry {
            sampled_ms: now_ms(),
            cpu_busy_percent: Some(37),
            mem_total_mib: 4096,
            mem_available_mib: 2000,
        });
        worker.telemetry_received_ms = Some(now_ms());
        let text = worker_text(&worker);
        assert!(text.contains("CPU busy: 37%"));
        assert!(text.contains("RAM available: 2000 / 4096 MiB"));
        assert!(text.contains("entire host"));
        assert!(text.to_lowercase().contains("reservations"));
    }
    #[test]
    fn worker_details_redact_sessions_and_label_telemetry() {
        let worker: Worker = serde_json::from_value(serde_json::json!({
            "registration": { "auth": {"id": "worker-1", "session": "SECRET-SESSION"},
                "capacity": {"cpu": 4, "mem_mib": 1024},
                "physical": {"cpu": 8, "mem_mib": 2048}, "allow_native": true,
                "gpus": [{"id":"gpu-1", "name":"test", "memory_mib":128,
                    "free_memory_mib":64, "utilization_percent":50, "simulated":true}] },
            "state": "online", "last_seen_ms": 0, "reserved": {"cpu":2,"mem_mib":128}
        }))
        .unwrap();
        let text = worker_text(&worker);
        assert!(!text.contains("SECRET-SESSION"));
        assert!(text.contains("SIMULATED"));
        assert!(text.contains("not measured CPU/RAM usage"));
        let mut app = App::new();
        app.workers.push(worker);
        app.view = View::Workers;
        app.rebuild();
        assert!(screen(&mut app, 160, 40).contains("Worker details"));
    }

    #[test]
    fn help_scroll_reaches_all_keys_on_small_terminals() {
        let mut app = App::new();
        key(&mut app, KeyCode::Char('?'));
        key(&mut app, KeyCode::End);
        assert!(screen(&mut app, 64, 12).contains("SIGTERM"));
        key(&mut app, KeyCode::Home);
        assert!(screen(&mut app, 64, 12).contains("Jobs / workers overview"));
        assert!(!screen(&mut app, 1, 1).contains('\u{1b}'));
    }

    #[tokio::test]
    async fn log_pane_uses_bounded_api_tail_and_sanitizes_output() {
        use axum::{Json, Router, extract::Query, routing::get};
        let router = Router::new()
            .route(
                "/jobs/legacy",
                get(|| async { Json(job("legacy", 1, JobState::Running)) }),
            )
            .route(
                "/jobs/job-1",
                get(|| async { Json(job("job-1", 1, JobState::Running)) }),
            )
            .route(
                "/jobs/job-1/logs/chunk",
                get(|Query(query): Query<LogQuery>| async move {
                    assert_eq!(query.tail, Some(TEXT_LIMIT));
                    assert_eq!(query.limit, Some(TEXT_LIMIT));
                    Json(serde_json::json!({
                        "stdout":"hello\u{1b}[2J", "stderr":"error", "attempt_id":"attempt-1",
                        "stdout_offset":0,"stderr_offset":0,"stdout_next":10,"stderr_next":5,
                        "stdout_total":10,"stderr_total":5,"reset":false,
                        "stdout_truncated":false,"stderr_truncated":false
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let client = Client::new(format!("http://{address}"), "x".repeat(32)).unwrap();
        let update = detail_update(
            client.clone(),
            DetailKey {
                id: "job-1".into(),
                pane: Pane::Stdout,
            },
        )
        .await;
        let legacy = detail_update(
            client,
            DetailKey {
                id: "legacy".into(),
                pane: Pane::Stdout,
            },
        )
        .await;
        server.abort();
        match update {
            Update::Detail { job, text, .. } => {
                assert!(job.is_ok());
                let text = text.unwrap();
                assert!(text.contains("attempt-1"));
                assert!(text.contains("hello"));
                assert!(!text.contains('\u{1b}'));
            }
            _ => panic!("expected detail update"),
        }
        match legacy {
            Update::Detail { job, text, .. } => {
                assert!(job.is_ok());
                assert!(
                    text.unwrap_err()
                        .to_string()
                        .contains("upgrade the scheduler or use runspk logs legacy")
                );
            }
            _ => panic!("expected legacy detail error"),
        }
    }
}
