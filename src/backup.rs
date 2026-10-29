use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::config::Job;
use crate::filter::Filter;
use crate::s3::S3;
use crate::sync::{mtime_ns, walk};

const ZSTD_LEVEL: i32 = 3;

#[derive(Debug)]
pub struct Report {
    pub key: String,
    pub files: usize,
    /// Compressed archive size.
    pub size: u64,
    pub uploaded: bool,
    /// Files that could not be read and are missing from the archive.
    pub unreadable: usize,
}

/// Archives the job's source into `<prefix><name>.tar.zst` and uploads it, unless the
/// object already in S3 has the same content hash. History is left to bucket versioning.
pub async fn run(job: &Job, s3: &S3, tmp_dir: &Path) -> Result<Report> {
    let key = job.key_for(&format!("{}.tar.zst", job.name));
    match s3.versioning_enabled(&job.bucket).await {
        Ok(true) => {}
        Ok(false) => warn!(
            job = %job.name,
            "bucket versioning is off: each backup replaces the previous one"
        ),
        Err(e) => warn!(job = %job.name, "could not check bucket versioning: {e:#}"),
    }

    std::fs::create_dir_all(tmp_dir).with_context(|| format!("creating {}", tmp_dir.display()))?;
    let tmp = tempfile::Builder::new()
        .prefix(&format!("backup-{}-", job.name))
        .suffix(".tar.zst.tmp")
        .tempfile_in(tmp_dir)
        .with_context(|| format!("creating temp file in {}", tmp_dir.display()))?;

    let archive = {
        let (job, file) = (job.clone(), tmp.reopen()?);
        tokio::task::spawn_blocking(move || build(&job, file)).await??
    };
    let hex = archive.hash.to_hex().to_string();
    let size = tmp.as_file().metadata()?.len();
    info!(job = %job.name, files = archive.files, size, "archive built");

    let uploaded = s3.remote_hash(&job.bucket, &key).await?.as_deref() != Some(hex.as_str());
    if uploaded {
        s3.upload(&job.bucket, &key, tmp.path(), size, &hex).await?;
        info!(job = %job.name, key = %key, "backup uploaded");
    } else {
        info!(job = %job.name, key = %key, "unchanged since last backup, not uploaded");
    }
    Ok(Report {
        key,
        files: archive.files,
        size,
        uploaded,
        unreadable: archive.unreadable,
    })
}

struct Archive {
    /// BLAKE3 of the uncompressed tar stream, so zstd version/level changes don't
    /// force a re-upload.
    hash: blake3::Hash,
    files: usize,
    unreadable: usize,
}

fn build(job: &Job, out: File) -> Result<Archive> {
    let filter = Filter::new(&job.exclude)?;
    let mut files = walk(&job.source, &job.source, &filter, job.follow_symlinks);
    // Stable order, so unchanged content gives an identical archive.
    files.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let enc = zstd::Encoder::new(out, ZSTD_LEVEL)?;
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
        }
    }

    fn archive(job: &Job) -> (Archive, Vec<u8>) {
        let out = tempfile::tempfile().unwrap();
        let a = build(job, out.try_clone().unwrap()).unwrap();
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
