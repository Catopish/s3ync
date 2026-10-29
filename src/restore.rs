use std::io::{self, Read, Seek};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use tracing::{error, info, warn};

use crate::backup;
use crate::config::{Config, ENCRYPTED_SUFFIX, Job, Mode, Naming};
use crate::crypto::{self, maybe_decrypt};
use crate::s3::S3;

pub struct Opts {
    pub job: String,
    pub to: Option<PathBuf>,
    pub identity: Option<PathBuf>,
    pub list: bool,
    pub at: Option<String>,
    pub version_id: Option<String>,
    pub path: Option<String>,
    pub force: bool,
}

/// Private keys, loaded (and their passphrase asked for) on the first encrypted object.
struct Keys {
    path: Option<PathBuf>,
    ids: OnceLock<std::result::Result<Vec<Box<dyn age::Identity + Send + Sync>>, String>>,
}

impl Keys {
    fn get(&self) -> Result<&[Box<dyn age::Identity + Send + Sync>]> {
        self.ids
            .get_or_init(|| match &self.path {
                None => {
                    Err("object is encrypted: pass --identity, or set encryption.identity".into())
                }
                Some(p) => crypto::load_identities(p).map_err(|e| format!("{e:#}")),
            })
            .as_deref()
            .map_err(|e| anyhow!("{e}"))
    }
}

pub async fn restore(cfg: &Config, s3: &S3, opts: Opts) -> Result<()> {
    let job = cfg
        .jobs
        .iter()
        .find(|j| j.name == opts.job)
        .ok_or_else(|| anyhow!("no job named {:?} in config", opts.job))?;
    let keys = Arc::new(Keys {
        path: opts
            .identity
            .clone()
            .or_else(|| cfg.encryption.identity.clone()),
        ids: OnceLock::new(),
    });
    match job.mode {
        Mode::Backup => {
            if opts.path.is_some() {
                bail!("--path only applies to sync jobs; a backup is restored whole");
            }
            if opts.list {
                return list_backups(job, s3).await;
            }
            restore_backup(job, s3, &opts, keys).await
        }
        Mode::Sync => {
            if opts.list || opts.at.is_some() || opts.version_id.is_some() {
                bail!("--list, --at and --version-id only apply to backup jobs");
            }
            restore_sync(cfg, job, s3, &opts, keys).await
        }
    }
}

async fn list_backups(job: &Job, s3: &S3) -> Result<()> {
    match job.naming {
        Naming::Timestamp => {
            let keys = backup::timestamped(job, s3).await?;
            if keys.is_empty() {
                println!("no backups for job {:?}", job.name);
            }
            for k in keys.iter().rev() {
                let enc = if k.ends_with(ENCRYPTED_SUFFIX) {
                    "  (encrypted)"
                } else {
                    ""
                };
                println!("{}  s3://{}/{k}{enc}", backup::stamp_of(k), job.bucket);
            }
        }
        Naming::Fixed => {
            let mut any = false;
            for key in backup::fixed_keys(job) {
                if !s3.exists(&job.bucket, &key).await? {
                    continue;
                }
                any = true;
                println!("s3://{}/{key}", job.bucket);
                match s3.versions(&job.bucket, &key).await {
                    Ok(vs) => {
                        for v in vs {
                            println!(
                                "  {}  {:>10}  --version-id {}{}",
                                local_time(v.modified_unix),
                                human_size(v.size),
                                v.id,
                                if v.latest { "  (latest)" } else { "" }
                            );
                        }
                    }
                    Err(e) => println!("  (no version history: {e:#})"),
                }
            }
            if !any {
                println!("no backups for job {:?}", job.name);
            }
        }
    }
    Ok(())
}

async fn restore_backup(job: &Job, s3: &S3, opts: &Opts, keys: Arc<Keys>) -> Result<()> {
    let to = opts.to.as_deref().context("--to is required")?;
    let key = match job.naming {
        Naming::Timestamp => {
            if opts.version_id.is_some() {
                bail!("--version-id is for naming = \"fixed\"; use --at with timestamped backups");
            }
            let keys = backup::timestamped(job, s3).await?;
            let at = opts.at.as_deref().unwrap_or("");
            keys.into_iter()
                .rev()
                .find(|k| backup::stamp_of(k).starts_with(at))
                .ok_or_else(|| match at {
                    "" => anyhow!("no backups for job {:?}", job.name),
                    _ => anyhow!("no backup matching --at {at:?} (see --list)"),
                })?
        }
        Naming::Fixed => {
            if opts.at.is_some() {
                bail!("--at is for naming = \"timestamp\"; use --version-id (see --list)");
            }
            // Prefer the format the job writes now; the other is from before a toggle.
            let mut candidates = backup::fixed_keys(job);
            if job.encrypted {
                candidates.reverse();
            }
            let mut found = None;
            for k in candidates {
                if s3.exists(&job.bucket, &k).await? {
                    found = Some(k);
                    break;
                }
            }
            found.ok_or_else(|| anyhow!("no backup for job {:?}", job.name))?
        }
    };

    prepare_dir(to, opts.force)?;
    info!(key = %key, to = %to.display(), "restoring backup");
    let file = tempfile::tempfile_in(to)
        .with_context(|| format!("creating temp file in {}", to.display()))?;
    let mut dst = tokio::fs::File::from_std(file.try_clone()?);
    let expected = s3
        .download(&job.bucket, &key, opts.version_id.as_deref(), &mut dst)
        .await?;
    drop(dst);

    let to_owned = to.to_path_buf();
    let (files, encrypted) = tokio::task::spawn_blocking(move || -> Result<_> {
        let mut file = file;
        file.rewind()?;
        let (plain, encrypted) = maybe_decrypt(file, || keys.get())?;
        let mut hashed = HashReader {
            inner: zstd::Decoder::new(plain)?,
            hasher: blake3::Hasher::new(),
        };
        let mut archive = tar::Archive::new(&mut hashed);
        archive.set_preserve_mtime(true);
        archive.set_overwrite(true);
        let mut files = 0;
        // `unpack_in` refuses entries that would land outside `to` (`..`, absolute paths).
        for entry in archive.entries()? {
            let mut entry = entry?;
            if entry.unpack_in(&to_owned)? && entry.header().entry_type().is_file() {
                files += 1;
            }
        }
        // Hash the whole tar stream, including the end-of-archive padding.
        io::copy(&mut hashed, &mut io::sink())?;
        if let Some(want) = expected {
            let got = hashed.hasher.finalize().to_hex();
            if got.as_str() != want {
                bail!(
                    "archive is corrupt or was modified (hash {got}, expected {want}); \
                     do not trust the files restored into {}",
                    to_owned.display()
                );
            }
        }
        Ok((files, encrypted))
    })
    .await??;

    println!(
        "restored {files} files from s3://{}/{key}{} into {}",
        job.bucket,
        if encrypted { " (decrypted)" } else { "" },
        to.display()
    );
    Ok(())
}

async fn restore_sync(
    cfg: &Config,
    job: &Job,
    s3: &S3,
    opts: &Opts,
    keys: Arc<Keys>,
) -> Result<()> {
    let to = opts.to.as_deref().context("--to is required")?;
    let sub = opts.path.as_deref().unwrap_or("").trim_matches('/');
    let prefix = job.key_for(sub);
    let objects = s3.list(&job.bucket, &prefix).await?;
    // With --path, only that file or that directory, not siblings sharing its prefix.
    let objects: Vec<String> = objects
        .into_keys()
        .filter(|k| {
            let rest = &k[prefix.len()..];
            sub.is_empty() || rest.is_empty() || rest == ENCRYPTED_SUFFIX || rest.starts_with('/')
        })
        .filter(|k| !k.ends_with('/'))
        .collect();
    if objects.is_empty() {
        bail!("nothing under s3://{}/{prefix}", job.bucket);
    }
    std::fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
    info!(objects = objects.len(), to = %to.display(), "restoring files");

    let results: Vec<Result<Restored>> = futures_util::stream::iter(objects)
        .map(|key| {
            let keys = keys.clone();
            async move {
                let rel = &key[job.prefix.len()..];
                let res = restore_object(job, s3, &key, rel, to, opts.force, keys).await;
                if let Err(e) = &res {
                    error!(key = %key, "{e:#}");
                }
                res
            }
        })
        .buffer_unordered(cfg.global.concurrency)
        .collect()
        .await;

    let (mut restored, mut decrypted, mut skipped, mut failed) = (0, 0, 0, 0);
    for r in results {
        match r {
            Ok(Restored::File { decrypted: d }) => {
                restored += 1;
                decrypted += usize::from(d);
            }
            Ok(Restored::Exists) => skipped += 1,
            Err(_) => failed += 1,
        }
    }
    println!(
        "restored {restored} files ({decrypted} decrypted) into {}, {skipped} skipped \
         (already exist, use --force), {failed} failed",
        to.display()
    );
    if failed > 0 {
        bail!("{failed} files failed to restore");
    }
    Ok(())
}

enum Restored {
    File { decrypted: bool },
    Exists,
}

async fn restore_object(
    job: &Job,
    s3: &S3,
    key: &str,
    rel: &str,
    to: &Path,
    force: bool,
    keys: Arc<Keys>,
) -> Result<Restored> {
    let rel_path = safe_rel(rel).ok_or_else(|| anyhow!("refusing unsafe path {rel:?}"))?;
    // The name without `.age` is used if the content turns out to be encrypted.
    let plain_dest = rel
        .strip_suffix(ENCRYPTED_SUFFIX)
        .map(|r| to.join(r))
        .unwrap_or_else(|| to.join(&rel_path));
    if !force && plain_dest.exists() {
        return Ok(Restored::Exists);
    }
    let dir = plain_dest.parent().unwrap_or(to).to_path_buf();
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;

    let tmp = tempfile::NamedTempFile::new_in(&dir)?;
    let mut dst = tokio::fs::File::from_std(tmp.reopen()?);
    s3.download(&job.bucket, key, None, &mut dst).await?;
    drop(dst);

    let (as_is, plain_dest) = (to.join(&rel_path), plain_dest);
    tokio::task::spawn_blocking(move || -> Result<Restored> {
        let (mut plain, encrypted) = maybe_decrypt(tmp.reopen()?, || keys.get())?;
        if !encrypted {
            drop(plain);
            tmp.persist(&as_is)?;
            return Ok(Restored::File { decrypted: false });
        }
        let mut out = tempfile::NamedTempFile::new_in(&dir)?;
        io::copy(&mut plain, &mut out)
            .with_context(|| format!("decrypting {}", as_is.display()))?;
        out.as_file().sync_all()?;
        out.persist(&plain_dest)?;
        Ok(Restored::File { decrypted: true })
    })
    .await?
}

/// `rel` as a relative path with only normal components; S3 keys can hold anything.
fn safe_rel(rel: &str) -> Option<PathBuf> {
    let p = Path::new(rel);
    let ok = !rel.is_empty() && p.components().all(|c| matches!(c, Component::Normal(_)));
    ok.then(|| p.to_path_buf())
}

/// Creates `dir`; unless `force`, it must be missing or empty.
fn prepare_dir(dir: &Path, force: bool) -> Result<()> {
    if dir.exists() {
        let non_empty = std::fs::read_dir(dir)
            .with_context(|| format!("reading {}", dir.display()))?
            .next()
            .is_some();
        if non_empty && !force {
            bail!(
                "{} is not empty (use --force to restore into it anyway)",
                dir.display()
            );
        }
        if non_empty {
            warn!(dir = %dir.display(), "restoring into a non-empty directory, files will be overwritten");
        }
    }
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))
}

struct HashReader<R> {
    inner: R,
    hasher: blake3::Hasher,
}

impl<R: Read> Read for HashReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

fn local_time(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|t| {
            t.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "?".into())
}

fn human_size(n: u64) -> String {
    match n {
        n if n < 1024 * 1024 => format!("{:.1} KiB", n as f64 / 1024.0),
        n if n < 1024 * 1024 * 1024 => format!("{:.1} MiB", n as f64 / 1048576.0),
        n => format!("{:.1} GiB", n as f64 / 1073741824.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsafe_paths_rejected() {
        assert!(safe_rel("a/b.txt").is_some());
        assert!(safe_rel("../etc/passwd").is_none());
        assert!(safe_rel("a/../../x").is_none());
        assert!(safe_rel("/abs").is_none());
        assert!(safe_rel("./a").is_none());
        assert!(safe_rel("").is_none());
    }

    #[test]
    fn refuses_non_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        prepare_dir(dir.path(), false).unwrap();
        std::fs::write(dir.path().join("x"), "").unwrap();
        assert!(prepare_dir(dir.path(), false).is_err());
        prepare_dir(dir.path(), true).unwrap();
        prepare_dir(&dir.path().join("new/sub"), false).unwrap();
    }
}
