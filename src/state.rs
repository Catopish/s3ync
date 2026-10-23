use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use redb::{Database, ReadableDatabase, TableDefinition};

/// Key: "<job>\0<relative path>", value: encoded `Entry`.
const FILES: TableDefinition<&str, &[u8]> = TableDefinition::new("files");

/// What we last uploaded for a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub size: u64,
    pub mtime_ns: i64,
    pub hash: [u8; 32],
}

impl Entry {
    const LEN: usize = 8 + 8 + 32;

    fn encode(&self) -> [u8; Self::LEN] {
        let mut b = [0u8; Self::LEN];
        b[..8].copy_from_slice(&self.size.to_le_bytes());
        b[8..16].copy_from_slice(&self.mtime_ns.to_le_bytes());
        b[16..].copy_from_slice(&self.hash);
        b
    }

    fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != Self::LEN {
            return None;
        }
        Some(Self {
            size: u64::from_le_bytes(b[..8].try_into().ok()?),
            mtime_ns: i64::from_le_bytes(b[8..16].try_into().ok()?),
            hash: b[16..].try_into().ok()?,
        })
    }
}

/// Local record of what has been synced, so restarts don't re-hash/re-upload everything.
/// All methods block (disk I/O); call them from blocking contexts.
#[derive(Clone)]
pub struct State {
    db: Arc<Database>,
}

fn key(job: &str, rel: &str) -> String {
    format!("{job}\0{rel}")
}

impl State {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating state dir {}", dir.display()))?;
        let path = dir.join("state.redb");
        let db = Database::create(&path).with_context(|| {
            format!(
                "opening state db {} (is s3ync already running?)",
                path.display()
            )
        })?;
        let txn = db.begin_write()?;
        txn.open_table(FILES)?;
        txn.commit()?;
        Ok(Self { db: Arc::new(db) })
    }

    pub fn get(&self, job: &str, rel: &str) -> Result<Option<Entry>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(FILES)?;
        Ok(table
            .get(key(job, rel).as_str())?
            .and_then(|v| Entry::decode(v.value())))
    }

    pub fn put(&self, job: &str, rel: &str, entry: &Entry) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(FILES)?;
            table.insert(key(job, rel).as_str(), entry.encode().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn remove(&self, job: &str, rel: &str) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(FILES)?;
            table.remove(key(job, rel).as_str())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// All tracked paths of a job that start with `prefix` ("" for all), with their entries.
    pub fn list(&self, job: &str, prefix: &str) -> Result<Vec<(String, Entry)>> {
        let start = key(job, prefix);
        let txn = self.db.begin_read()?;
        let table = txn.open_table(FILES)?;
        let job_len = job.len() + 1;
        let mut out = Vec::new();
        for item in table.range(start.as_str()..)? {
            let (k, v) = item?;
            let k = k.value();
            if !k.starts_with(&start) {
                break;
            }
            if let Some(e) = Entry::decode(v.value()) {
                out.push((k[job_len..].to_string(), e));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_prefix_list() {
        let dir = tempfile::tempdir().unwrap();
        let s = State::open(dir.path()).unwrap();
        let e = Entry {
            size: 5,
            mtime_ns: -3,
            hash: [7; 32],
        };
        s.put("a", "x/1", &e).unwrap();
        s.put("a", "x/2", &e).unwrap();
        s.put("a", "y", &e).unwrap();
        s.put("ab", "x/3", &e).unwrap();
        assert_eq!(s.get("a", "x/1").unwrap(), Some(e));
        assert_eq!(s.get("b", "x/1").unwrap(), None);

        let xs: Vec<_> = s
            .list("a", "x/")
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(xs, ["x/1", "x/2"]);
        assert_eq!(s.list("a", "").unwrap().len(), 3);

        s.remove("a", "x/1").unwrap();
        assert_eq!(s.get("a", "x/1").unwrap(), None);
    }
}
