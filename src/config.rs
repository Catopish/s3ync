use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use croner::Cron;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub global: Global,
    #[serde(rename = "job", default)]
    pub jobs: Vec<Job>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Global {
    pub state_dir: PathBuf,
    pub log_level: String,
    /// Max concurrent uploads/deletes across all jobs.
    pub concurrency: usize,
    /// Full reconcile interval, catches anything the watcher missed. 0 disables.
    pub rescan_interval_secs: u64,
    /// Files at or above this size use multipart upload.
    pub multipart_threshold_mb: u64,
    pub part_size_mb: u64,
    /// Needed for most S3-compatible stores (MinIO etc). Endpoint comes from AWS_ENDPOINT_URL.
    pub force_path_style: bool,
}

impl Default for Global {
    fn default() -> Self {
        Self {
            state_dir: PathBuf::from("/var/lib/s3ync"),
            log_level: "info".into(),
            concurrency: 4,
            rescan_interval_secs: 3600,
            multipart_threshold_mb: 16,
            part_size_mb: 16,
            force_path_style: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Sync,
    Backup,
}

/// How backup archives are named in the bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Naming {
    /// `<prefix><name>.tar.zst`, overwritten each run; history comes from bucket versioning.
    #[default]
    Fixed,
    /// `<prefix><name>/<name>-<UTC time>.tar.zst`, for buckets without versioning (e.g. R2).
    Timestamp,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub name: String,
    pub mode: Mode,
    pub bucket: String,
    pub source: PathBuf,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub delete: bool,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default)]
    pub follow_symlinks: bool,
    #[serde(default)]
    pub naming: Naming,
    /// Timestamped backups to keep, oldest are deleted after a run. 0 keeps all.
    #[serde(default)]
    pub keep_last: u32,
    /// Cron expression (local time) for backups taken by the daemon, e.g. "0 2 * * *".
    /// Without it the job only runs via `s3ync backup`.
    pub schedule: Option<String>,
    #[serde(skip)]
    pub cron: Option<Cron>,
}

fn default_debounce_ms() -> u64 {
    3000
}

impl Job {
    pub fn debounce(&self) -> Duration {
        Duration::from_millis(self.debounce_ms)
    }

    /// S3 key for a path relative to `source` (always `/`-separated).
    pub fn key_for(&self, rel: &str) -> String {
        format!("{}{}", self.prefix, rel)
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut cfg: Config =
            toml::from_str(&raw).with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&mut self) -> Result<()> {
        if self.jobs.is_empty() {
            bail!("no [[job]] entries in config");
        }
        if self.global.concurrency == 0 {
            bail!("global.concurrency must be at least 1");
        }
        // S3 minimum part size is 5 MiB (except the last part).
        if self.global.part_size_mb < 5 {
            bail!("global.part_size_mb must be at least 5");
        }

        let mut names = HashSet::new();
        for job in &mut self.jobs {
            let name = job.name.clone();
            if name.is_empty()
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                bail!("job name {name:?} must be non-empty and only contain [A-Za-z0-9_-]");
            }
            if !names.insert(name.clone()) {
                bail!("duplicate job name {name:?}");
            }
            if job.mode == Mode::Backup && job.delete {
                bail!("job {name:?}: delete is not supported for mode = \"backup\"");
            }
            if job.mode != Mode::Backup && (job.naming != Naming::Fixed || job.keep_last > 0) {
                bail!("job {name:?}: naming and keep_last only apply to mode = \"backup\"");
            }
            if let Some(expr) = &job.schedule {
                if job.mode != Mode::Backup {
                    bail!("job {name:?}: schedule only applies to mode = \"backup\"");
                }
                let cron = expr
                    .parse::<Cron>()
                    .with_context(|| format!("job {name:?}: invalid schedule {expr:?}"))?;
                if cron
                    .find_next_occurrence(&chrono::Local::now(), false)
                    .is_err()
                {
                    bail!("job {name:?}: schedule {expr:?} never fires");
                }
                job.cron = Some(cron);
            }
            if job.naming == Naming::Fixed && job.keep_last > 0 {
                bail!(
                    "job {name:?}: keep_last needs naming = \"timestamp\" \
                     (fixed naming keeps history through bucket versioning)"
                );
            }
            if job.bucket.is_empty() {
                bail!("job {name:?}: bucket is empty");
            }
            if !job.source.is_absolute() {
                bail!("job {name:?}: source must be an absolute path");
            }
            if !job.source.is_dir() {
                bail!(
                    "job {name:?}: source {} is not a directory",
                    job.source.display()
                );
            }
            job.prefix = normalize_prefix(&job.prefix);
        }
        Ok(())
    }
}

/// "/a/b" -> "a/b/", "" -> "".
fn normalize_prefix(p: &str) -> String {
    let p = p.trim_matches('/');
    if p.is_empty() {
        String::new()
    } else {
        format!("{p}/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_normalization() {
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("/"), "");
        assert_eq!(normalize_prefix("photos"), "photos/");
        assert_eq!(normalize_prefix("/a/b/"), "a/b/");
    }

    #[test]
    fn parses_example() {
        let dir = tempfile::tempdir().unwrap();
        let toml = format!(
            r#"
            [global]
            state_dir = "/tmp/x"

            [[job]]
            name = "photos"
            mode = "sync"
            bucket = "b"
            source = "{}"
            prefix = "/photos"
            exclude = ["*.tmp"]
            "#,
            dir.path().display()
        );
        let mut cfg: Config = toml::from_str(&toml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.jobs[0].prefix, "photos/");
        assert_eq!(cfg.jobs[0].key_for("a/b.jpg"), "photos/a/b.jpg");
        assert_eq!(cfg.global.concurrency, 4);
    }

    #[test]
    fn backup_rejects_delete() {
        let dir = tempfile::tempdir().unwrap();
        let toml = |delete| {
            format!(
                "[[job]]\nname = \"x\"\nmode = \"backup\"\nbucket = \"b\"\nsource = \"{}\"\ndelete = {delete}\n",
                dir.path().display()
            )
        };
        let mut cfg: Config = toml::from_str(&toml(false)).unwrap();
        cfg.validate().unwrap();
        let mut cfg: Config = toml::from_str(&toml(true)).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn keep_last_needs_timestamp_naming() {
        let dir = tempfile::tempdir().unwrap();
        let toml = |mode, extra| {
            format!(
                "[[job]]\nname = \"x\"\nmode = \"{mode}\"\nbucket = \"b\"\nsource = \"{}\"\n{extra}\n",
                dir.path().display()
            )
        };
        let ok = |mode, extra| {
            let mut cfg: Config = toml::from_str(&toml(mode, extra)).unwrap();
            cfg.validate().is_ok()
        };
        assert!(ok("backup", "naming = \"timestamp\"\nkeep_last = 7"));
        assert!(ok("backup", "naming = \"timestamp\""));
        assert!(!ok("backup", "keep_last = 7"));
        assert!(!ok("sync", "naming = \"timestamp\""));
        assert!(ok("backup", "schedule = \"0 2 * * *\""));
        assert!(!ok("backup", "schedule = \"0 25 * * *\""));
        assert!(!ok("backup", "schedule = \"0 0 30 2 *\""));
        assert!(!ok("sync", "schedule = \"0 2 * * *\""));
    }
}
