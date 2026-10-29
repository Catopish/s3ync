mod backup;
mod config;
mod filter;
mod s3;
mod state;
mod sync;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use sd_notify::NotifyState;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use config::{Config, Mode, Naming};
use s3::S3;
use state::State;
use sync::{JobCtx, JobStatus};

#[derive(Parser)]
#[command(version, about = "Sync local directories to S3")]
struct Cli {
    #[arg(
        long,
        short,
        env = "S3YNC_CONFIG",
        default_value = "/etc/s3ync/config.toml",
        global = true
    )]
    config: PathBuf,
    /// .env file with AWS credentials. Defaults to ./.env if present.
    #[arg(long, env = "S3YNC_ENV", global = true)]
    env: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the daemon (watch and sync continuously).
    Run,
    /// Validate the config and check bucket access.
    Check,
    /// Do a single reconcile pass and exit.
    Once {
        /// Only this job.
        #[arg(long)]
        job: Option<String>,
    },
    /// Show per-job status of the running daemon.
    Status,
    /// Archive backup jobs and upload them (meant for a timer or cron).
    Backup {
        /// Only this job.
        #[arg(long)]
        job: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.env {
        Some(p) => {
            dotenvy::from_path(p).with_context(|| format!("loading env file {}", p.display()))?;
        }
        None => {
            let _ = dotenvy::dotenv();
        }
    }
    let cfg = Config::load(&cli.config)?;

    let filter = EnvFilter::try_from_env("S3YNC_LOG")
        .unwrap_or_else(|_| EnvFilter::new(format!("s3ync={}", cfg.global.log_level)));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        match cli.cmd {
            Cmd::Run => run(cfg).await,
            Cmd::Check => check(cfg).await,
            Cmd::Once { job } => once(cfg, job).await,
            Cmd::Status => status(&cfg),
            Cmd::Backup { job } => backup(cfg, job).await,
        }
    })
}

async fn check(cfg: Config) -> Result<()> {
    let s3 = S3::new(&cfg.global).await?;
    let mut ok = true;
    for job in &cfg.jobs {
        match s3.check(&job.bucket, &job.prefix).await {
            Ok(()) => {
                println!(
                    "ok    {:<16} {} -> s3://{}/{}",
                    job.name,
                    job.source.display(),
                    job.bucket,
                    job.prefix
                );
                if let Some(cron) = &job.cron
                    && let Ok(next) = cron.find_next_occurrence(&chrono::Local::now(), false)
                {
                    println!("      next backup: {}", next.format("%Y-%m-%d %H:%M %Z"));
                }
                if job.mode == Mode::Backup && job.naming == Naming::Fixed {
                    match s3.versioning_enabled(&job.bucket).await {
                        Ok(true) => {}
                        Ok(false) => println!(
                            "      note: versioning is off, each backup replaces the previous one"
                        ),
                        Err(e) => println!(
                            "      note: could not check versioning ({e:#})\n      \
                             without it each backup replaces the previous one; \
                             consider naming = \"timestamp\" (R2 has no versioning)"
                        ),
                    }
                }
            }
            Err(e) => {
                ok = false;
                println!("FAIL  {:<16} {e:#}", job.name);
            }
        }
    }
    if !ok {
        bail!("some jobs failed the check");
    }
    Ok(())
}

async fn build_jobs(cfg: &Config, only: Option<&str>) -> Result<Vec<Arc<JobCtx>>> {
    let s3 = S3::new(&cfg.global).await?;
    let state = State::open(&cfg.global.state_dir)?;
    let mut out = Vec::new();
    for job in &cfg.jobs {
        if job.mode != Mode::Sync || only.is_some_and(|n| n != job.name) {
            continue;
        }
        s3.check(&job.bucket, &job.prefix)
            .await
            .with_context(|| format!("job {:?}", job.name))?;
        out.push(Arc::new(JobCtx::new(
            job.clone(),
            s3.clone(),
            state.clone(),
        )?));
    }
    if let Some(n) = only
        && out.is_empty()
    {
        bail!("no sync job named {n:?}");
    }
    Ok(out)
}

async fn once(cfg: Config, only: Option<String>) -> Result<()> {
    let jobs = build_jobs(&cfg, only.as_deref()).await?;
    let mut failed = 0;
    for ctx in jobs {
        let (ok, bad) = sync::run_once(ctx.clone(), cfg.global.concurrency).await?;
        let st = ctx.status.lock().unwrap().clone();
        println!(
            "{}: {ok} ok ({} uploaded, {} deleted), {bad} failed",
            ctx.job.name, st.uploaded, st.deleted
        );
        failed += bad;
    }
    if failed > 0 {
        bail!("{failed} paths failed");
    }
    Ok(())
}

async fn backup(cfg: Config, only: Option<String>) -> Result<()> {
    let jobs: Vec<_> = cfg
        .jobs
        .iter()
        .filter(|j| j.mode == Mode::Backup && only.as_ref().is_none_or(|n| *n == j.name))
        .collect();
    if jobs.is_empty() {
        match only {
            Some(n) => bail!("no backup job named {n:?}"),
            None => bail!("no jobs with mode = \"backup\" in config"),
        }
    }
    let s3 = S3::new(&cfg.global).await?;
    let tmp = backup::tmp_dir(&cfg.global.state_dir);
    let mut failed = 0;
    for job in jobs {
        let started = sync::unix_now();
        match backup::run(job, &s3, &tmp).await {
            Ok(r) => {
                if let Err(e) = backup::record_success(&cfg.global.state_dir, &job.name, started) {
                    warn!(job = %job.name, "{e:#}");
                }
                println!(
                    "{}: {} files, {:.1} MiB -> s3://{}/{} ({})",
                    job.name,
                    r.files,
                    r.size as f64 / (1024.0 * 1024.0),
                    job.bucket,
                    r.key,
                    if r.uploaded { "uploaded" } else { "unchanged" }
                );
                if r.pruned > 0 {
                    println!("{}: deleted {} old backup(s)", job.name, r.pruned);
                }
                if r.unreadable > 0 {
                    println!("{}: {} unreadable files left out", job.name, r.unreadable);
                    failed += 1;
                }
            }
            Err(e) => {
                error!(job = %job.name, "backup failed: {e:#}");
                failed += 1;
            }
        }
    }
    if failed > 0 {
        bail!("{failed} backup job(s) failed or are incomplete");
    }
    Ok(())
}

async fn run(cfg: Config) -> Result<()> {
    let jobs = build_jobs(&cfg, None).await?;
    let scheduled: Vec<_> = cfg.jobs.iter().filter(|j| j.cron.is_some()).collect();
    if jobs.is_empty() && scheduled.is_empty() {
        bail!("nothing to run: no sync jobs and no backup jobs with a schedule");
    }
    let shutdown = CancellationToken::new();
    let sem = Arc::new(Semaphore::new(cfg.global.concurrency));
    let rescan = (cfg.global.rescan_interval_secs > 0)
        .then(|| Duration::from_secs(cfg.global.rescan_interval_secs));

    let (ready_tx, mut ready_rx) = mpsc::channel(jobs.len());
    let mut handles = Vec::new();
    for ctx in &jobs {
        let fut = sync::run_watch(
            ctx.clone(),
            sem.clone(),
            rescan,
            ready_tx.clone(),
            shutdown.clone(),
        );
        let name = ctx.job.name.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = fut.await {
                error!(job = %name, "{e:#}");
                // A job that can't watch is fatal: let systemd restart us.
                shutdown.cancel();
                return Err(e);
            }
            Ok(())
        }));
    }
    drop(ready_tx);

    let mut statuses: Vec<_> = jobs
        .iter()
        .map(|j| (j.job.name.clone(), j.status.clone()))
        .collect();
    if !scheduled.is_empty() {
        let s3 = S3::new(&cfg.global).await?;
        for job in scheduled {
            s3.check(&job.bucket, &job.prefix)
                .await
                .with_context(|| format!("job {:?}", job.name))?;
            let status = Arc::new(Mutex::new(JobStatus::default()));
            statuses.push((job.name.clone(), status.clone()));
            handles.push(tokio::spawn({
                let fut = backup::run_scheduled(
                    job.clone(),
                    s3.clone(),
                    cfg.global.state_dir.clone(),
                    sem.clone(),
                    status,
                    shutdown.clone(),
                );
                async move {
                    fut.await;
                    Ok(())
                }
            }));
        }
    }

    tokio::spawn(write_status_loop(
        statuses.clone(),
        cfg.global.state_dir.clone(),
        shutdown.clone(),
    ));

    // Ready once every job has its watcher up and its initial reconcile queued.
    tokio::select! {
        _ = async { for _ in 0..jobs.len() { ready_rx.recv().await; } } => {
            info!(jobs = jobs.len(), "ready");
            let _ = sd_notify::notify(&[NotifyState::Ready]);
        }
        _ = shutdown.cancelled() => {}
    }

    wait_for_signal(&shutdown).await;
    let _ = sd_notify::notify(&[NotifyState::Stopping]);
    info!("shutting down");
    shutdown.cancel();

    let mut res = Ok(());
    for h in handles {
        if let Ok(Err(e)) = h.await {
            res = Err(e);
        }
    }
    write_status(&statuses, &cfg.global.state_dir);
    res
}

async fn wait_for_signal(shutdown: &CancellationToken) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
        _ = shutdown.cancelled() => {}
    }
}

const STATUS_FILE: &str = "status.json";

type Statuses = Vec<(String, Arc<Mutex<JobStatus>>)>;

fn write_status(jobs: &Statuses, dir: &Path) {
    let map: BTreeMap<_, _> = jobs
        .iter()
        .map(|(name, st)| (name.clone(), st.lock().unwrap().clone()))
        .collect();
    let pending: usize = map.values().map(|s| s.pending).sum();
    let _ = sd_notify::notify(&[NotifyState::Status(&format!(
        "{} job(s), {pending} pending",
        map.len()
    ))]);
    let tmp = dir.join(format!("{STATUS_FILE}.tmp"));
    let res = serde_json::to_vec_pretty(&map)
        .map_err(anyhow::Error::from)
        .and_then(|b| Ok(std::fs::write(&tmp, b)?))
        .and_then(|_| Ok(std::fs::rename(&tmp, dir.join(STATUS_FILE))?));
    if let Err(e) = res {
        warn!("writing status file: {e:#}");
    }
}

async fn write_status_loop(jobs: Statuses, dir: PathBuf, shutdown: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = tick.tick() => write_status(&jobs, &dir),
            _ = shutdown.cancelled() => return,
        }
    }
}

fn status(cfg: &Config) -> Result<()> {
    let path = cfg.global.state_dir.join(STATUS_FILE);
    let raw = std::fs::read(&path)
        .with_context(|| format!("reading {} (has the daemon run yet?)", path.display()))?;
    let map: BTreeMap<String, JobStatus> = serde_json::from_slice(&raw)?;
    let age = std::fs::metadata(&path)?
        .modified()?
        .elapsed()
        .unwrap_or_default();
    if age > Duration::from_secs(30) {
        println!(
            "note: status is {}s old, the daemon may not be running\n",
            age.as_secs()
        );
    }
    let now = sync::unix_now();
    let is_backup = |name: &str| {
        cfg.jobs
            .iter()
            .any(|j| j.name == name && j.mode == Mode::Backup)
    };
    let (backups, syncs): (Vec<_>, Vec<_>) = map.iter().partition(|(n, _)| is_backup(n));
    if !syncs.is_empty() {
        println!(
            "{:<16} {:>8} {:>8} {:>8} {:>8}  last reconcile",
            "job", "pending", "uploaded", "deleted", "failed"
        );
    }
    for (name, s) in &syncs {
        let last = s.last_reconcile_unix.map_or_else(
            || "never".into(),
            |t| format!("{} ago", human(now.saturating_sub(t))),
        );
        println!(
            "{name:<16} {:>8} {:>8} {:>8} {:>8}  {last}",
            s.pending, s.uploaded, s.deleted, s.failed
        );
        if let Some(e) = &s.last_error {
            println!("  last error: {e}");
        }
    }
    if !backups.is_empty() {
        if !syncs.is_empty() {
            println!();
        }
        println!(
            "{:<16} {:>8} {:>8} {:>8}  {:<14} next",
            "backup job", "uploaded", "pruned", "failed", "last ok"
        );
    }
    for (name, s) in &backups {
        let last = s.last_backup_unix.map_or_else(
            || "never".into(),
            |t| format!("{} ago", human(now.saturating_sub(t))),
        );
        let next = s.next_backup_unix.map_or_else(
            || "-".into(),
            |t| match t.checked_sub(now) {
                Some(d) if d > 0 => format!("in {}", human(d)),
                _ => "now".into(),
            },
        );
        println!(
            "{name:<16} {:>8} {:>8} {:>8}  {last:<14} {next}",
            s.uploaded, s.deleted, s.failed
        );
        if let Some(e) = &s.last_error {
            println!("  last error: {e}");
        }
    }
    Ok(())
}

/// 42s, 5m, 3h, 2d
fn human(secs: u64) -> String {
    match secs {
        s if s < 120 => format!("{s}s"),
        s if s < 7200 => format!("{}m", s / 60),
        s if s < 172_800 => format!("{}h", s / 3600),
        s => format!("{}d", s / 86400),
    }
}
