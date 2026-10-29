use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use notify::EventKind;
use notify::event::ModifyKind;
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use serde::Serialize;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Job;
use crate::filter::Filter;
use crate::s3::S3;
use crate::state::{Entry, State};

const MAX_RETRY_DELAY: Duration = Duration::from_secs(300);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

pub struct JobCtx {
    pub job: Job,
    filter: Filter,
    s3: S3,
    state: State,
    pub status: Arc<Mutex<JobStatus>>,
}

/// Snapshot of a job's progress, written to `status.json` by the daemon.
#[derive(Debug, Default, Clone, Serialize, serde::Deserialize)]
pub struct JobStatus {
    pub uploaded: u64,
    pub deleted: u64,
    pub failed: u64,
    pub pending: usize,
    pub last_reconcile_unix: Option<u64>,
    pub last_error: Option<String>,
    /// Backup jobs only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_backup_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_backup_unix: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Uploaded,
    /// Content already matches S3; only local state was refreshed.
    Unchanged,
    /// Removed locally and deleted from S3.
    Deleted,
    /// Removed locally, S3 copy kept (`delete = false`).
    Forgotten,
    /// Nothing to do: excluded, a directory, a skipped symlink, unknown path, ...
    Skipped,
}

enum Work {
    /// A path that may have been created, modified or removed.
    Path(String),
    /// A directory that appeared (created or moved in); its contents need syncing.
    Dir(String),
    /// Full reconcile against S3.
    Rescan,
}

impl JobCtx {
    pub fn new(job: Job, s3: S3, state: State) -> Result<Self> {
        let filter = Filter::new(&job.exclude).with_context(|| format!("job {:?}", job.name))?;
        Ok(Self {
            job,
            filter,
            s3,
            state,
            status: Arc::default(),
        })
    }

    fn rel_of(&self, abs: &Path) -> Option<String> {
        let rel = abs.strip_prefix(&self.job.source).ok()?;
        let rel = rel.to_str()?;
        (!rel.is_empty()).then(|| rel.to_string())
    }

    /// Runs a state DB operation on the blocking pool.
    async fn st<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&State, &str) -> Result<T> + Send + 'static,
    {
        let state = self.state.clone();
        let job = self.job.name.clone();
        tokio::task::spawn_blocking(move || f(&state, &job)).await?
    }

    fn stat(&self, abs: &Path) -> std::io::Result<std::fs::Metadata> {
        if self.job.follow_symlinks {
            std::fs::metadata(abs)
        } else {
            std::fs::symlink_metadata(abs)
        }
    }

    /// Brings S3 in line with the current local state of one path.
    pub async fn sync_path(&self, rel: &str) -> Result<Outcome> {
        if self.filter.is_excluded(rel) {
            return Ok(Outcome::Skipped);
        }
        let abs = self.job.source.join(rel);
        let meta = match self.stat(&abs) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return self.removed(rel).await,
            Err(e) => return Err(e).with_context(|| format!("stat {}", abs.display())),
        };
        if !meta.is_file() {
            return Ok(Outcome::Skipped);
        }
        let (size, mtime) = (meta.len(), mtime_ns(&meta));

        let rel_owned = rel.to_string();
        let prev = self.st(move |s, j| s.get(j, &rel_owned)).await?;
        if let Some(p) = prev
            && p.size == size
            && p.mtime_ns == mtime
        {
            return Ok(Outcome::Skipped);
        }

        let hash = hash_file(abs.clone()).await?;
        let entry = Entry {
            size,
            mtime_ns: mtime,
            hash: *hash.as_bytes(),
        };
        let hex = hash.to_hex().to_string();
        let key = self.job.key_for(rel);

        let already_there = match prev {
            Some(p) => p.hash == entry.hash,
            // Unknown locally (fresh state DB): avoid re-uploading what S3 already has.
            None => {
                self.s3
                    .remote_hash(&self.job.bucket, &key)
                    .await?
                    .as_deref()
                    == Some(hex.as_str())
            }
        };
        if !already_there {
            self.s3
                .upload(&self.job.bucket, &key, &abs, size, &hex)
                .await?;
            // If the file changed while we read it, the upload may be torn: retry later.
            let after = self
                .stat(&abs)
                .with_context(|| format!("stat {}", abs.display()))?;
            if after.len() != size || mtime_ns(&after) != mtime {
                return Err(anyhow!(
                    "{} changed during upload, will retry",
                    abs.display()
                ));
            }
        }

        let rel_owned = rel.to_string();
        self.st(move |s, j| s.put(j, &rel_owned, &entry)).await?;
        Ok(if already_there {
            Outcome::Unchanged
        } else {
            Outcome::Uploaded
        })
    }

    /// `rel` no longer exists locally. It may have been a file or a whole directory.
    async fn removed(&self, rel: &str) -> Result<Outcome> {
        let (r1, r2) = (rel.to_string(), format!("{rel}/"));
        let mut gone: Vec<String> = self
            .st(move |s, j| Ok(s.list(j, &r2)?.into_iter().map(|(k, _)| k).collect()))
            .await?;
        if self.st(move |s, j| s.get(j, &r1)).await?.is_some() {
            gone.push(rel.to_string());
        }
        if gone.is_empty() {
            return Ok(Outcome::Skipped);
        }
        for r in gone {
            if self.job.delete {
                self.s3
                    .delete(&self.job.bucket, &self.job.key_for(&r))
                    .await?;
                info!(job = %self.job.name, path = %r, "deleted");
            }
            self.st(move |s, j| s.remove(j, &r)).await?;
        }
        Ok(if self.job.delete {
            Outcome::Deleted
        } else {
            Outcome::Forgotten
        })
    }

    /// Compares local files, local state and S3, returning paths that need `sync_path`.
    pub async fn reconcile(&self) -> Result<Vec<String>> {
        let remote = self.s3.list(&self.job.bucket, &self.job.prefix).await?;
        let mut tracked: HashMap<String, Entry> =
            self.st(|s, j| s.list(j, "")).await?.into_iter().collect();
        let local = {
            let (src, follow) = (self.job.source.clone(), self.job.follow_symlinks);
            let filter = Filter::new(&self.job.exclude)?;
            tokio::task::spawn_blocking(move || walk(&src, &src, &filter, follow)).await?
        };

        let mut todo = Vec::new();
        for (rel, size, mtime_ns) in local {
            let remote_ok = remote.get(&self.job.key_for(&rel)) == Some(&size);
            match tracked.remove(&rel) {
                Some(e) if remote_ok && e.size == size && e.mtime_ns == mtime_ns => continue,
                // Object missing or different in S3: forget it so it gets checked properly.
                Some(_) if !remote_ok => {
                    let r = rel.clone();
                    self.st(move |s, j| s.remove(j, &r)).await?;
                }
                _ => {}
            }
            todo.push(rel);
        }
        // Tracked but no longer present locally (or now excluded).
        todo.extend(tracked.into_keys().filter(|r| !self.filter.is_excluded(r)));

        self.status.lock().unwrap().last_reconcile_unix = Some(unix_now());
        Ok(todo)
    }

    fn record(&self, rel: &str, res: &Result<Outcome>) {
        let mut st = self.status.lock().unwrap();
        match res {
            Ok(Outcome::Uploaded) => {
                st.uploaded += 1;
                info!(job = %self.job.name, path = %rel, "uploaded");
            }
            Ok(Outcome::Deleted) => st.deleted += 1,
            Ok(o) => debug!(job = %self.job.name, path = %rel, outcome = ?o),
            Err(e) => {
                st.failed += 1;
                st.last_error = Some(format!("{rel}: {e:#}"));
            }
        }
    }
}

pub(crate) fn mtime_ns(m: &std::fs::Metadata) -> i64 {
    m.mtime() * 1_000_000_000 + m.mtime_nsec()
}

pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn hash_file(path: PathBuf) -> Result<blake3::Hash> {
    tokio::task::spawn_blocking(move || {
        let f =
            std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let mut h = blake3::Hasher::new();
        h.update_reader(f)
            .with_context(|| format!("reading {}", path.display()))?;
        Ok(h.finalize())
    })
    .await?
}

/// Regular files under `dir` (relative to `root`) that aren't excluded: (rel, size, mtime_ns).
pub(crate) fn walk(
    root: &Path,
    dir: &Path,
    filter: &Filter,
    follow: bool,
) -> Vec<(String, u64, i64)> {
    let rel_of = |p: &Path| {
        p.strip_prefix(root)
            .ok()
            .and_then(|r| r.to_str())
            .map(str::to_string)
    };
    let mut out = Vec::new();
    let it = walkdir::WalkDir::new(dir)
        .follow_links(follow)
        .into_iter()
        .filter_entry(|e| match rel_of(e.path()) {
            Some(r) => r.is_empty() || !filter.is_excluded(&r),
            None => true, // reported below
        });
    for entry in it {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                warn!("walk {}: {e}", root.display());
                continue;
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let Some(rel) = rel_of(entry.path()) else {
            warn!("skipping non UTF-8 path {}", entry.path().display());
            continue;
        };
        match entry.metadata() {
            Ok(m) => out.push((rel, m.len(), mtime_ns(&m))),
            Err(e) => warn!("stat {}: {e}", entry.path().display()),
        }
    }
    out
}

/// One reconcile pass, then exit. Returns (succeeded, failed).
pub async fn run_once(ctx: Arc<JobCtx>, concurrency: usize) -> Result<(usize, usize)> {
    let todo = ctx.reconcile().await?;
    info!(job = %ctx.job.name, pending = todo.len(), "reconciled");
    let results: Vec<bool> = futures_util::stream::iter(todo)
        .map(|rel| {
            let ctx = ctx.clone();
            async move {
                let mut attempt = 0;
                loop {
                    let res = ctx.sync_path(&rel).await;
                    ctx.record(&rel, &res);
                    match res {
                        Ok(_) => return true,
                        Err(e) if attempt < 2 => {
                            attempt += 1;
                            warn!(job = %ctx.job.name, path = %rel, "attempt {attempt} failed: {e:#}");
                            tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                        }
                        Err(e) => {
                            error!(job = %ctx.job.name, path = %rel, "giving up: {e:#}");
                            return false;
                        }
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let ok = results.iter().filter(|r| **r).count();
    Ok((ok, results.len() - ok))
}

/// Watches the job's source and keeps S3 in sync until `shutdown` fires.
/// `ready` is signalled once the watcher is up and the initial reconcile has been queued.
pub async fn run_watch(
    ctx: Arc<JobCtx>,
    sem: Arc<Semaphore>,
    rescan_every: Option<Duration>,
    ready: mpsc::Sender<()>,
    shutdown: CancellationToken,
) -> Result<()> {
    let name = ctx.job.name.clone();
    let (tx, mut rx) = mpsc::unbounded_channel::<Work>();

    // Start watching before the initial reconcile so nothing slips through in between.
    let mut debouncer = {
        let (ctx, tx) = (ctx.clone(), tx.clone());
        new_debouncer(ctx.job.debounce(), None, move |res: DebounceEventResult| {
            on_events(&ctx, &tx, res)
        })?
    };
    debouncer
        .watch(&ctx.job.source, notify::RecursiveMode::Recursive)
        .map_err(|e| watch_error(&ctx.job.source, e))?;
    info!(job = %name, source = %ctx.job.source.display(), "watching");

    let _ = tx.send(Work::Rescan);
    let mut ready = Some(ready);

    let mut inflight: HashSet<String> = HashSet::new();
    let mut again: HashSet<String> = HashSet::new();
    let mut attempts: HashMap<String, u32> = HashMap::new();
    let mut tasks: JoinSet<(String, Result<Outcome>)> = JoinSet::new();
    let mut reconciling: Option<JoinHandle<()>> = None;
    let mut rescan = tokio::time::interval(rescan_every.unwrap_or(Duration::MAX / 4));
    rescan.tick().await; // first tick is immediate; initial rescan is already queued

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = rescan.tick(), if rescan_every.is_some() => { let _ = tx.send(Work::Rescan); }
            Some(work) = rx.recv() => match work {
                Work::Path(rel) => {
                    if !inflight.insert(rel.clone()) {
                        again.insert(rel);
                        continue;
                    }
                    let (ctx, sem) = (ctx.clone(), sem.clone());
                    tasks.spawn(async move {
                        let _permit = sem.acquire_owned().await;
                        let res = ctx.sync_path(&rel).await;
                        (rel, res)
                    });
                }
                Work::Dir(rel) => {
                    let (ctx, tx) = (ctx.clone(), tx.clone());
                    tokio::spawn(async move {
                        let filter = match Filter::new(&ctx.job.exclude) { Ok(f) => f, Err(_) => return };
                        let src = ctx.job.source.clone();
                        let files = tokio::task::spawn_blocking(move || {
                            walk(&src, &src.join(&rel), &filter, ctx.job.follow_symlinks)
                        }).await.unwrap_or_default();
                        for (r, _, _) in files {
                            let _ = tx.send(Work::Path(r));
                        }
                    });
                }
                Work::Rescan => {
                    if reconciling.as_ref().is_some_and(|h| !h.is_finished()) {
                        continue;
                    }
                    let (ctx, tx, ready) = (ctx.clone(), tx.clone(), ready.take());
                    reconciling = Some(tokio::spawn(async move {
                        match ctx.reconcile().await {
                            Ok(todo) => {
                                info!(job = %ctx.job.name, pending = todo.len(), "reconciled");
                                for r in todo {
                                    let _ = tx.send(Work::Path(r));
                                }
                            }
                            Err(e) => {
                                error!(job = %ctx.job.name, "reconcile failed: {e:#}");
                                ctx.status.lock().unwrap().last_error = Some(format!("reconcile: {e:#}"));
                            }
                        }
                        if let Some(r) = ready { let _ = r.send(()).await; }
                    }));
                }
            },
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                let Ok((rel, res)) = joined else { continue };
                inflight.remove(&rel);
                ctx.record(&rel, &res);
                if let Err(e) = &res {
                    let n = attempts.entry(rel.clone()).or_insert(0);
                    *n += 1;
                    let delay = Duration::from_secs(1u64 << (*n).min(9)).min(MAX_RETRY_DELAY);
                    warn!(job = %name, path = %rel, attempt = *n, retry_in = ?delay, "sync failed: {e:#}");
                    let tx = tx.clone();
                    let r = rel.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = tx.send(Work::Path(r));
                    });
                } else {
                    attempts.remove(&rel);
                }
                if again.remove(&rel) {
                    let _ = tx.send(Work::Path(rel));
                }
            }
        }
        ctx.status.lock().unwrap().pending = inflight.len() + again.len();
    }

    drop(debouncer);
    if let Some(h) = reconciling {
        h.abort();
    }
    if !tasks.is_empty() {
        info!(job = %name, inflight = tasks.len(), "waiting for in-flight transfers");
        let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
            while let Some(Ok((rel, res))) = tasks.join_next().await {
                ctx.record(&rel, &res);
            }
        })
        .await;
        if drained.is_err() {
            warn!(job = %name, "aborting {} transfers after {SHUTDOWN_GRACE:?}", tasks.len());
            tasks.abort_all();
        }
    }
    Ok(())
}

/// Runs on the debouncer's thread: turn filesystem events into work items.
fn on_events(ctx: &JobCtx, tx: &mpsc::UnboundedSender<Work>, res: DebounceEventResult) {
    let events = match res {
        Ok(ev) => ev,
        Err(errs) => {
            for e in errs {
                error!(job = %ctx.job.name, "watch error: {}", watch_error(&ctx.job.source, e));
            }
            // Something may have been missed.
            let _ = tx.send(Work::Rescan);
            return;
        }
    };
    for ev in events {
        if ev.need_rescan() {
            let _ = tx.send(Work::Rescan);
            continue;
        }
        let appeared = match ev.kind {
            EventKind::Access(_) => continue,
            EventKind::Create(_) | EventKind::Modify(ModifyKind::Name(_)) => true,
            _ => false,
        };
        for p in &ev.paths {
            let Some(rel) = ctx.rel_of(p) else { continue };
            if ctx.filter.is_excluded(&rel) {
                continue;
            }
            let is_dir = std::fs::symlink_metadata(p).is_ok_and(|m| m.is_dir());
            let _ = tx.send(if appeared && is_dir {
                Work::Dir(rel)
            } else {
                Work::Path(rel)
            });
        }
    }
}

fn watch_error(source: &Path, e: notify::Error) -> anyhow::Error {
    if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) {
        anyhow!(
            "inotify watch limit reached while watching {}; raise it with \
             `sysctl fs.inotify.max_user_watches=524288` (persist in /etc/sysctl.d/)",
            source.display()
        )
    } else {
        anyhow!(e).context(format!("watching {}", source.display()))
    }
}
