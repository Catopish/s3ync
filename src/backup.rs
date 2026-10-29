use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{ENCRYPTED_SUFFIX, Job, Naming};
use crate::crypto::Crypto;
use crate::filter::Filter;
use crate::s3::S3;
use crate::sync::{JobStatus, mtime_ns, unix_now, walk};

const ZSTD_LEVEL: i32 = 3;
const RETRY_BASE: Duration = Duration::from_secs(60);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(3600);
/// Last successful run per job, so a restarted daemon catches up on a missed run and
/// encrypted backups (whose objects carry no hash) can still skip unchanged uploads.
const LAST_RUN_FILE: &str = "backups.json";
const ARCHIVE_EXT: &str = ".tar.zst";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LastRun {
    pub at: u64,
    /// Hash of the archive's tar stream and the key it is stored at.
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
}

/// Takes the job's backups on its `schedule` until `shutdown`. A run missed while the
/// daemon was down happens right away; a failed run is retried with backoff until the
/// next scheduled time.
pub async fn run_scheduled(
    job: Job,
    s3: S3,
    crypto: Option<Arc<Crypto>>,
    state_dir: PathBuf,
    sem: Arc<Semaphore>,
    status: Arc<Mutex<JobStatus>>,
    shutdown: CancellationToken,
) {
    let cron = job
        .cron
        .clone()
        .expect("only jobs with a schedule are scheduled");
    let next_after = |t: DateTime<Local>| cron.find_next_occurrence(&t, false);
    let mut last = last_runs(&state_dir).get(&job.name).map(|r| r.at);
    status.lock().unwrap().last_backup_unix = last;
    let mut failures = 0u32;

    loop {
        let now = Local::now();
        let due = if failures > 0 {
            let backoff = (RETRY_BASE * 2u32.saturating_pow(failures - 1)).min(MAX_RETRY_DELAY);
            next_after(now).map(|n| n.min(now + backoff))
        } else {
            next_after(last.and_then(local_time).unwrap_or(now))
        };
        let due = match due {
            Ok(d) => d,
            Err(e) => {
                error!(job = %job.name, "no next run for schedule: {e}");
                return;
            }
        };
        status.lock().unwrap().next_backup_unix = Some(due.timestamp().max(0) as u64);

        // Short steps: tokio's clock stops during system suspend, the wall clock doesn't.
        while let Ok(left) = (due - Local::now()).to_std() {
            tokio::select! {
                _ = tokio::time::sleep(left.min(Duration::from_secs(60))) => {}
                _ = shutdown.cancelled() => return,
            }
        }
        if failures == 0 && due < now {
            info!(job = %job.name, scheduled = %due, "missed a scheduled backup, running it now");
        }

        let _permit = tokio::select! {
            p = sem.acquire() => p.expect("semaphore closed"),
            _ = shutdown.cancelled() => return,
        };
        let res = tokio::select! {
            r = run(&job, &s3, crypto.clone(), &state_dir) => r,
            _ = shutdown.cancelled() => {
                warn!(job = %job.name, "backup interrupted by shutdown");
                return;
            }
        };
        let mut st = status.lock().unwrap();
        match res {
            Ok(r) => {
                failures = 0;
                last = Some(r.at);
                st.last_backup_unix = last;
                st.uploaded += u64::from(r.uploaded);
                st.deleted += r.pruned as u64;
                if r.unreadable > 0 {
                    st.last_error = Some(format!("{} unreadable files left out", r.unreadable));
                }
            }
            Err(e) => {
                failures += 1;
                error!(job = %job.name, "backup failed: {e:#}");
                st.failed += 1;
                st.last_error = Some(format!("backup: {e:#}"));
            }
        }
    }
}

fn local_time(unix: u64) -> Option<DateTime<Local>> {
    DateTime::from_timestamp(unix as i64, 0).map(|t| t.with_timezone(&Local))
}

fn last_runs(state_dir: &Path) -> HashMap<String, LastRun> {
    std::fs::read(state_dir.join(LAST_RUN_FILE))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

fn record_success(state_dir: &Path, job: &str, run: LastRun) -> Result<()> {
    let mut runs = last_runs(state_dir);
    runs.insert(job.to_string(), run);
    // Per-process temp name: `s3ync backup` may run next to the daemon.
    let tmp = state_dir.join(format!("{LAST_RUN_FILE}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, serde_json::to_vec_pretty(&runs)?)
        .and_then(|_| std::fs::rename(&tmp, state_dir.join(LAST_RUN_FILE)))
        .with_context(|| format!("recording backup time in {}", state_dir.display()))
}

#[derive(Debug)]
pub struct Report {
    /// When the run started (unix secs).
    pub at: u64,
    pub key: String,
    pub files: usize,
    /// Compressed archive size.
    pub size: u64,
    pub uploaded: bool,
    /// Files that could not be read and are missing from the archive.
    pub unreadable: usize,
    /// Old timestamped backups deleted because of `keep_last`.
    pub pruned: usize,
}

/// Archives the job's source and uploads it, unless the latest backup is identical.
/// With `Naming::Fixed` history is left to bucket versioning, with `Naming::Timestamp`
/// every upload gets its own key and `keep_last` prunes old ones. Encrypted jobs need
/// `crypto`. Records the run in `state_dir`.
pub async fn run(
    job: &Job,
    s3: &S3,
    crypto: Option<Arc<Crypto>>,
    state_dir: &Path,
) -> Result<Report> {
    let at = unix_now();
    let crypto = crypto.filter(|_| job.encrypted);
    if job.encrypted && crypto.is_none() {
        bail!(
            "job {:?} is encrypted but no encryption keys are loaded",
            job.name
        );
    }
    let ext = archive_ext(job);
    let (key, previous) = match job.naming {
        Naming::Fixed => {
            match s3.versioning_enabled(&job.bucket).await {
                Ok(true) => {}
                Ok(false) => warn!(
                    job = %job.name,
                    "bucket versioning is off: each backup replaces the previous one"
                ),
                Err(e) => warn!(
                    job = %job.name,
                    "could not check bucket versioning ({e:#}); without it each backup \
                     replaces the previous one, consider naming = \"timestamp\""
                ),
            }
            let key = job.key_for(&format!("{}{ext}", job.name));
            (key.clone(), Some(key))
        }
        Naming::Timestamp => {
            let key = job.key_for(&format!("{0}/{0}-{1}{ext}", job.name, stamp(at)));
            (key, timestamped(job, s3).await?.pop())
        }
    };

    let tmp_dir = tmp_dir(state_dir);
    std::fs::create_dir_all(&tmp_dir).with_context(|| format!("creating {}", tmp_dir.display()))?;
    let tmp = tempfile::Builder::new()
        .prefix(&format!("backup-{}-", job.name))
        .suffix(".tmp")
        .tempfile_in(&tmp_dir)
        .with_context(|| format!("creating temp file in {}", tmp_dir.display()))?;

    let archive = {
        let (job, file, crypto) = (job.clone(), tmp.reopen()?, crypto.clone());
        tokio::task::spawn_blocking(move || build(&job, file, crypto.as_deref())).await??
    };
    let hex = archive.hash.to_hex().to_string();
    let size = tmp.as_file().metadata()?.len();
    info!(job = %job.name, files = archive.files, size, "archive built");

    let unchanged = match &previous {
        // Encrypted objects carry no hash: compare with what this machine last uploaded.
        Some(prev) if crypto.is_some() => {
            let last = last_runs(state_dir).remove(&job.name).unwrap_or_default();
            last.hash.as_deref() == Some(hex.as_str())
                && last.key.as_deref() == Some(prev.as_str())
                && s3.exists(&job.bucket, prev).await?
        }
        Some(prev) => s3.remote_hash(&job.bucket, prev).await?.as_deref() == Some(hex.as_str()),
        None => false,
    };
    let key = if unchanged {
        let prev = previous.unwrap();
        info!(job = %job.name, key = %prev, "unchanged since last backup, not uploaded");
        prev
    } else {
        let meta_hash = crypto.is_none().then_some(hex.as_str());
        s3.upload(&job.bucket, &key, tmp.path(), size, meta_hash)
            .await?;
        info!(job = %job.name, key = %key, "backup uploaded");
        key
    };
    let run = LastRun {
        at,
        hash: Some(hex),
        key: Some(key.clone()),
    };
    if let Err(e) = record_success(state_dir, &job.name, run) {
        warn!(job = %job.name, "{e:#}");
    }

    let mut pruned = 0;
    if job.naming == Naming::Timestamp && job.keep_last > 0 {
        let all = timestamped(job, s3).await?;
        let excess = all.len().saturating_sub(job.keep_last as usize);
        for old in &all[..excess] {
            s3.delete(&job.bucket, old).await?;
            info!(job = %job.name, key = %old, "old backup deleted");
            pruned += 1;
        }
    }

    Ok(Report {
        at,
        key,
        files: archive.files,
        size,
        uploaded: !unchanged,
        unreadable: archive.unreadable,
        pruned,
    })
}

fn archive_ext(job: &Job) -> String {
    if job.encrypted {
        format!("{ARCHIVE_EXT}{ENCRYPTED_SUFFIX}")
    } else {
        ARCHIVE_EXT.to_string()
    }
}

/// Keys the job's fixed-name backup may be stored at (plain, encrypted).
pub fn fixed_keys(job: &Job) -> [String; 2] {
    let k = job.key_for(&format!("{}{ARCHIVE_EXT}", job.name));
    [k.clone(), k + ENCRYPTED_SUFFIX]
}

/// Keys of the job's timestamped backups (plain or encrypted), oldest first. Anything
/// else under the job's folder is ignored, so it is never picked as "latest" nor pruned.
pub async fn timestamped(job: &Job, s3: &S3) -> Result<Vec<String>> {
    let dir = job.key_for(&format!("{}/", job.name));
    let lead = format!("{}-", job.name);
    let mut keys: Vec<String> = s3
        .list(&job.bucket, &dir)
        .await?
        .into_keys()
        .filter(|k| {
            let r = &k[dir.len()..];
            r.strip_prefix(&lead)
                .map(|r| r.strip_suffix(ENCRYPTED_SUFFIX).unwrap_or(r))
                .and_then(|r| r.strip_suffix(ARCHIVE_EXT))
                .is_some_and(is_stamp)
        })
        .collect();
    // The stamp is fixed-width and most-significant first, so this is chronological.
    keys.sort_unstable_by(|a, b| stamp_of(a).cmp(stamp_of(b)).then(a.cmp(b)));
    Ok(keys)
}

/// UTC `YYYY-MM-DDTHH-MM-SSZ` (no `:`, which is awkward in keys and file names).
fn stamp(unix: u64) -> String {
    let (days, secs) = ((unix / 86400) as i64, unix % 86400);
    // Days since epoch -> civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}-{:02}-{:02}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

/// The `YYYY-...Z` part of a timestamped backup key.
pub fn stamp_of(key: &str) -> &str {
    let name = key.rsplit('/').next().unwrap_or(key);
    let name = name.strip_suffix(ENCRYPTED_SUFFIX).unwrap_or(name);
    let name = name.strip_suffix(ARCHIVE_EXT).unwrap_or(name);
    &name[name.len().saturating_sub(STAMP_LEN)..]
}

const STAMP_LEN: usize = 20;

fn is_stamp(s: &str) -> bool {
    const SHAPE: &[u8] = b"0000-00-00T00-00-00Z";
    s.len() == SHAPE.len()
        && s.bytes().zip(SHAPE).all(|(c, &p)| {
            if p == b'0' {
                c.is_ascii_digit()
            } else {
                c == p
            }
        })
}

struct Archive {
    /// BLAKE3 of the uncompressed tar stream, so zstd version/level changes don't
    /// force a re-upload.
    hash: blake3::Hash,
    files: usize,
    unreadable: usize,
}

/// The archive file, optionally behind age encryption.
enum Sink {
    Plain(File),
    Age(age::stream::StreamWriter<File>),
}

impl Sink {
    fn finish(self) -> io::Result<File> {
        match self {
            Sink::Plain(f) => Ok(f),
            Sink::Age(w) => w.finish(),
        }
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Sink::Plain(f) => f.write(buf),
            Sink::Age(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Sink::Plain(f) => f.flush(),
            Sink::Age(w) => w.flush(),
        }
    }
}

/// tar -> (hash) -> zstd -> [age] -> `out`
fn build(job: &Job, out: File, crypto: Option<&Crypto>) -> Result<Archive> {
    let filter = Filter::new(&job.exclude)?;
    let mut files = walk(&job.source, &job.source, &filter, job.follow_symlinks);
    // Stable order, so unchanged content gives an identical archive.
    files.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let sink = match crypto {
        Some(c) => Sink::Age(c.encrypt_to(out)?),
        None => Sink::Plain(out),
    };
    let enc = zstd::Encoder::new(sink, ZSTD_LEVEL)?;
    let mut tar = tar::Builder::new(HashWriter {
        inner: enc,
        hasher: blake3::Hasher::new(),
    });
    let (mut added, mut unreadable) = (0, 0);
    for (rel, _, _) in &files {
        let abs = job.source.join(rel);
        match append(&mut tar, &abs, rel) {
            Ok(()) => added += 1,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                unreadable += 1;
                warn!(job = %job.name, path = %rel, "skipped: {e}");
            }
        }
    }
    let w = tar.into_inner().context("finishing tar")?;
    w.inner
        .finish()
        .and_then(Sink::finish)
        .and_then(|f| f.sync_all())
        .context("finishing archive")?;
    Ok(Archive {
        hash: w.hasher.finalize(),
        files: added,
        unreadable,
    })
}

fn append<W: Write>(tar: &mut tar::Builder<W>, abs: &Path, rel: &str) -> io::Result<()> {
    let file = File::open(abs)?;
    let meta = file.metadata()?;
    let size = meta.len();
    let mut header = tar::Header::new_gnu();
    header.set_metadata_in_mode(&meta, tar::HeaderMode::Complete);
    header.set_size(size);
    // Exactly `size` bytes even if the file grows or shrinks meanwhile, so the archive stays valid.
    let data = (&file).take(size).chain(io::repeat(0)).take(size);
    tar.append_data(&mut header, rel, data)?;

    let after = file.metadata()?;
    if after.len() != size || mtime_ns(&after) != mtime_ns(&meta) {
        warn!(path = %abs.display(), "changed while being archived, the copy may be inconsistent");
    }
    Ok(())
}

/// Hashes everything written through it.
struct HashWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Where archives are staged before upload.
pub fn tmp_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("tmp")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Mode;

    fn job(source: &Path) -> Job {
        Job {
            name: "t".into(),
            mode: Mode::Backup,
            bucket: "b".into(),
            source: source.to_path_buf(),
            prefix: String::new(),
            delete: false,
            exclude: vec!["*.tmp".into()],
            debounce_ms: 0,
            follow_symlinks: false,
            naming: Naming::Fixed,
            keep_last: 0,
            schedule: None,
            cron: None,
            encrypt: true,
            encrypted: false,
        }
    }

    #[test]
    fn stamps() {
        assert_eq!(stamp(0), "1970-01-01T00-00-00Z");
        assert_eq!(stamp(951_782_400), "2000-02-29T00-00-00Z");
        assert_eq!(stamp(1_793_275_335), "2026-10-29T12-02-15Z");
        assert_eq!(stamp(4_102_444_799), "2099-12-31T23-59-59Z");
        assert!(is_stamp(&stamp(1_793_275_335)));
        assert!(!is_stamp("manual"));
        assert!(!is_stamp("2026-10-29T12:02:15Z"));
    }

    fn archive(job: &Job) -> (Archive, Vec<u8>) {
        let out = tempfile::tempfile().unwrap();
        let a = build(job, out.try_clone().unwrap(), None).unwrap();
        let mut bytes = Vec::new();
        let mut out = out;
        io::Seek::rewind(&mut out).unwrap();
        out.read_to_end(&mut bytes).unwrap();
        (a, bytes)
    }

    #[test]
    fn hash_is_stable_and_tracks_content() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("a.txt"), "hello").unwrap();
        std::fs::write(dir.path().join("sub/b.txt"), "world").unwrap();
        std::fs::write(dir.path().join("skip.tmp"), "x").unwrap();
        let job = job(dir.path());

        let (a1, bytes) = archive(&job);
        let (a2, _) = archive(&job);
        assert_eq!(a1.hash, a2.hash);
        assert_eq!(a1.files, 2);

        let mut tar = tar::Archive::new(zstd::Decoder::new(&bytes[..]).unwrap());
        let mut names: Vec<String> = tar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["a.txt", "sub/b.txt"]);

        std::fs::write(dir.path().join("a.txt"), "hellO").unwrap();
        let (a3, _) = archive(&job);
        assert_ne!(a1.hash, a3.hash);
    }
}
