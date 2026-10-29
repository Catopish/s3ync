mod backup;
mod config;
mod crypto;
mod filter;
mod restore;
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
use crypto::Crypto;
use s3::S3;
use state::State;
use sync::{JobCtx, JobStatus};

const EXAMPLES: &str = "\
Examples:
  s3ync check                                   validate config.toml, test bucket access
  s3ync run                                     daemon: live sync + scheduled backups
  s3ync backup --job db-dumps                   take one backup now
  s3ync keygen                                  create the age key for [encryption]
  s3ync restore --job db-dumps --list           show available backups
  s3ync restore --job db-dumps --to /tmp/db     restore the latest backup
  s3ync restore --job photos --to /tmp/p -i ~/.ssh/id_ed25519

Config: /etc/s3ync/config.toml (see dist/config.example.toml).
Credentials: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT_URL
from the environment or the --env file. Logging: S3YNC_LOG=s3ync=debug.";

#[derive(Parser)]
#[command(
    version,
    about = "Sync directories to S3 and take scheduled, optionally encrypted backups",
    long_about = "Sync directories to S3 and take scheduled, optionally encrypted backups.\n\n\
        Everything is configured in one config.toml: sync jobs are watched and uploaded \
        within seconds, backup jobs are packed into .tar.zst archives on their cron \
        schedule. With [encryption] type = \"age\", objects are encrypted before upload.",
    after_help = EXAMPLES
)]
struct Cli {
    /// Config file
    #[arg(
        long,
        short,
        env = "S3YNC_CONFIG",
        default_value = "/etc/s3ync/config.toml",
        value_name = "FILE",
        global = true
    )]
    config: PathBuf,
    /// .env file with AWS credentials [default: ./.env if present]
    #[arg(long, env = "S3YNC_ENV", value_name = "FILE", global = true)]
    env: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the daemon: live sync and scheduled backups until SIGTERM
    #[command(long_about = "Run the daemon until SIGTERM.\n\n\
        Sync jobs: watch the source and upload changes within seconds, with a full \
        re-check on startup and every global.rescan_interval_secs.\n\
        Backup jobs with a `schedule`: take a backup at each cron time; a run missed \
        while the daemon was down happens right after startup.")]
    Run,
    /// Validate the config, check bucket access and encryption keys
    Check,
    /// One sync pass for sync jobs, then exit (non-zero if anything failed)
    Once {
        /// Only this job
        #[arg(long, value_name = "NAME")]
        job: Option<String>,
    },
    /// Show per-job status of the running daemon
    Status,
    /// Take backups now, then exit (non-zero if any failed)
    #[command(
        long_about = "Take backups now, then exit (non-zero if any failed).\n\n\
        Packs each backup job's source into a .tar.zst archive (encrypted to .tar.zst.age \
        with [encryption]) and uploads it, unless nothing changed since the last backup."
    )]
    Backup {
        /// Only this job [default: all backup jobs]
        #[arg(long, value_name = "NAME")]
        job: Option<String>,
    },
    /// Restore a backup, or a sync job's files, from S3
    #[command(
        long_about = "Restore from S3 into a local directory.\n\n\
            Backup jobs: unpacks the latest backup, or the one picked with --at / \
            --version-id (see --list).\n\
            Sync jobs: downloads every file, or only those under --path.\n\n\
            Encrypted objects are recognized by content and decrypted with --identity \
            (or encryption.identity from the config). The source directories in the \
            config need not exist, so this works on a fresh machine.",
        after_help = "Examples:\n  \
            s3ync restore --job db-dumps --list\n  \
            s3ync restore --job db-dumps --to /tmp/db --at 2026-10-29\n  \
            s3ync restore --job photos --to /tmp/photos --path 2024/summer -i ~/.ssh/id_ed25519"
    )]
    Restore {
        /// Job to restore (bucket, prefix and kind come from the config)
        #[arg(long, value_name = "NAME")]
        job: String,
        /// Directory to restore into; created if missing, must be empty unless --force
        #[arg(long, value_name = "DIR", required_unless_present = "list")]
        to: Option<PathBuf>,
        /// Private key for encrypted objects: age identity file or SSH private key
        /// [default: encryption.identity]. Passphrase: prompted, or S3YNC_PASSPHRASE
        #[arg(long, short, env = "S3YNC_IDENTITY", value_name = "FILE")]
        identity: Option<PathBuf>,
        /// List available backups instead of restoring (backup jobs)
        #[arg(long)]
        list: bool,
        /// Timestamped backup to restore: its UTC time or a prefix of it, e.g. 2026-10-29
        /// for that day's last backup [default: latest]
        #[arg(long, value_name = "TIME", conflicts_with = "version_id")]
        at: Option<String>,
        /// Object version to restore, from --list (naming = "fixed" on a versioned bucket)
        #[arg(long, value_name = "ID")]
        version_id: Option<String>,
        /// Only restore this file or directory (sync jobs)
        #[arg(long, value_name = "PATH")]
        path: Option<String>,
        /// Restore into a non-empty directory, overwriting existing files
        #[arg(long)]
        force: bool,
    },
    /// Create an age key pair for [encryption]
    #[command(long_about = "Create an age key pair for [encryption].\n\n\
        Writes the private key (mode 0600, never overwrites) and prints the public key. \
        Copy the private key somewhere safe off this machine: without it, nothing \
        encrypted can be restored.")]
    Keygen {
        /// Where to write the private key [default: encryption.identity from the config]
        #[arg(long, short, value_name = "FILE")]
        output: Option<PathBuf>,
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
    // Commands that don't read the sources must work where they don't exist.
    let needs_sources = matches!(
        cli.cmd,
        Cmd::Run | Cmd::Check | Cmd::Once { .. } | Cmd::Backup { .. }
    );
    let cfg = match &cli.cmd {
        Cmd::Keygen { output: Some(_) } => None,
        _ => Some(Config::load(&cli.config, needs_sources)?),
    };

    let level = cfg.as_ref().map_or("info", |c| c.global.log_level.as_str());
    let filter = EnvFilter::try_from_env("S3YNC_LOG")
        .unwrap_or_else(|_| EnvFilter::new(format!("s3ync={level}")));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    if let Cmd::Keygen { output } = cli.cmd {
        return keygen(cfg.as_ref(), output);
    }
    let cfg = cfg.expect("loaded above");
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        match cli.cmd {
            Cmd::Run => run(cfg).await,
            Cmd::Check => check(cfg).await,
            Cmd::Once { job } => once(cfg, job).await,
            Cmd::Status => status(&cfg),
            Cmd::Backup { job } => backup(cfg, job).await,
            Cmd::Restore {
                job,
                to,
                identity,
                list,
                at,
                version_id,
                path,
                force,
            } => {
                let s3 = S3::new(&cfg.global).await?;
                let opts = restore::Opts {
                    job,
                    to,
                    identity,
                    list,
                    at,
                    version_id,
                    path,
                    force,
                };
                restore::restore(&cfg, &s3, opts).await
            }
            Cmd::Keygen { .. } => unreachable!(),
        }
    })
}

fn keygen(cfg: Option<&Config>, output: Option<PathBuf>) -> Result<()> {
    let configured = cfg.and_then(|c| c.encryption.identity.clone());
    let path = output
        .or_else(|| configured.clone())
        .context("no output: pass --output FILE or set encryption.identity in the config")?;
    let public = crypto::keygen(&path)?;
    println!("private key: {}", path.display());
    println!("public key:  {public}");
    println!(
        "\nIMPORTANT: copy {} somewhere safe outside this machine (password manager, \
         offline drive). Without it, encrypted data cannot be restored.",
        path.display()
    );
    if configured.as_deref() != Some(path.as_path()) {
        println!(
            "To use it, set encryption.identity = \"{}\", or add the public key to \
             encryption.recipients.",
            path.display()
        );
    }
    Ok(())
}

/// Encryption keys, if any job is encrypted.
fn load_crypto(cfg: &Config) -> Result<Option<Arc<Crypto>>> {
    if !cfg.jobs.iter().any(|j| j.encrypted) {
        return Ok(None);
    }
    Ok(Crypto::load(&cfg.encryption)?.map(Arc::new))
}

async fn check(cfg: Config) -> Result<()> {
    let s3 = S3::new(&cfg.global).await?;
    let mut ok = true;
    match load_crypto(&cfg) {
        Ok(Some(c)) => {
            println!(
                "ok    encryption: age, {} recipient(s)",
                c.recipient_count()
            );
            if let Some(id) = &cfg.encryption.identity
                && id.exists()
            {
                println!(
                    "      note: keep a copy of {} off this machine, restores need it",
                    id.display()
                );
            }
        }
        Ok(None) => {}
        Err(e) => {
            ok = false;
            println!("FAIL  encryption: {e:#}");
        }
    }
    for job in &cfg.jobs {
        match s3.check(&job.bucket, &job.prefix).await {
            Ok(()) => {
                println!(
                    "ok    {:<16} {} -> s3://{}/{}{}",
                    job.name,
                    job.source.display(),
                    job.bucket,
                    job.prefix,
                    if job.encrypted { "  (encrypted)" } else { "" }
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

async fn build_jobs(
    cfg: &Config,
    crypto: Option<Arc<Crypto>>,
    only: Option<&str>,
) -> Result<Vec<Arc<JobCtx>>> {
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
            crypto.clone(),
            backup::tmp_dir(&cfg.global.state_dir),
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
    let jobs = build_jobs(&cfg, load_crypto(&cfg)?, only.as_deref()).await?;
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
    let crypto = load_crypto(&cfg)?;
    let mut failed = 0;
    for job in jobs {
        match backup::run(job, &s3, crypto.clone(), &cfg.global.state_dir).await {
            Ok(r) => {
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
    let crypto = load_crypto(&cfg)?;
    let jobs = build_jobs(&cfg, crypto.clone(), None).await?;
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
                    crypto.clone(),
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
