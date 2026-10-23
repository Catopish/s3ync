mod config;
mod filter;
mod s3;
mod state;
mod sync;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use sd_notify::NotifyState;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use config::{Config, Mode};
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
        }
    })
}

async fn check(cfg: Config) -> Result<()> {
    let s3 = S3::new(&cfg.global).await?;
    let mut ok = true;
    for job in &cfg.jobs {
        match s3.check(&job.bucket, &job.prefix).await {
            Ok(()) => println!(
                "ok    {:<16} {} -> s3://{}/{}",
                job.name,
                job.source.display(),
                job.bucket,
                job.prefix
            ),
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

async fn run(cfg: Config) -> Result<()> {
    let jobs = build_jobs(&cfg, None).await?;
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

    tokio::spawn(write_status_loop(
        jobs.clone(),
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
    write_status(&jobs, &cfg.global.state_dir);
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

fn write_status(jobs: &[Arc<JobCtx>], dir: &Path) {
    let map: BTreeMap<_, _> = jobs
        .iter()
        .map(|j| (j.job.name.clone(), j.status.lock().unwrap().clone()))
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

async fn write_status_loop(jobs: Vec<Arc<JobCtx>>, dir: PathBuf, shutdown: CancellationToken) {
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
    println!(
        "{:<16} {:>8} {:>8} {:>8} {:>8}  last reconcile",
        "job", "pending", "uploaded", "deleted", "failed"
    );
    for (name, s) in &map {
        let last = s
            .last_reconcile_unix
            .map(|t| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(t);
                format!("{}s ago", now.saturating_sub(t))
            })
            .unwrap_or_else(|| "never".into());
        println!(
            "{name:<16} {:>8} {:>8} {:>8} {:>8}  {last}",
            s.pending, s.uploaded, s.deleted, s.failed
        );
        if let Some(e) = &s.last_error {
            println!("  last error: {e}");
        }
    }
    Ok(())
}
