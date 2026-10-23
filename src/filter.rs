use anyhow::{Context, Result};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

/// Gitignore-ish exclude rules, matched against `/`-separated paths relative to the job source.
///
/// - `*.tmp`        matches at any depth
/// - `.git/`        a directory at any depth (and everything inside it)
/// - `build/out`    contains a `/`, so it is anchored to the source root
/// - `**/x`         explicit any-depth pattern
pub struct Filter {
    set: GlobSet,
}

impl Filter {
    pub fn new(patterns: &[String]) -> Result<Self> {
        let mut b = GlobSetBuilder::new();
        for raw in patterns {
            for p in expand(raw) {
                let glob = GlobBuilder::new(&p)
                    .literal_separator(true)
                    .build()
                    .with_context(|| format!("invalid exclude pattern {raw:?}"))?;
                b.add(glob);
            }
        }
        Ok(Self { set: b.build()? })
    }

    pub fn is_excluded(&self, rel: &str) -> bool {
        self.set.is_match(rel)
    }
}

fn expand(raw: &str) -> Vec<String> {
    let p = raw.trim_end_matches('/');
    let anchored = p.contains('/') && !p.starts_with("**/");
    let p = p.trim_start_matches('/');
    let base = if anchored || p.starts_with("**/") {
        p.to_string()
    } else {
        format!("**/{p}")
    };
    // Also match everything below a matching directory.
    vec![format!("{base}/**"), base]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(p: &[&str]) -> Filter {
        Filter::new(&p.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn any_depth() {
        let f = f(&["*.tmp"]);
        assert!(f.is_excluded("a.tmp"));
        assert!(f.is_excluded("x/y/a.tmp"));
        assert!(!f.is_excluded("a.tmpx"));
    }

    #[test]
    fn directories() {
        let f = f(&[".git/", "**/node_modules/"]);
        assert!(f.is_excluded(".git"));
        assert!(f.is_excluded(".git/config"));
        assert!(f.is_excluded("sub/.git/HEAD"));
        assert!(f.is_excluded("a/node_modules/x/y.js"));
        assert!(!f.is_excluded("git/config"));
    }

    #[test]
    fn anchored() {
        let f = f(&["build/out"]);
        assert!(f.is_excluded("build/out/a"));
        assert!(!f.is_excluded("x/build/out/a"));
    }

    #[test]
    fn star_does_not_cross_dirs() {
        let f = f(&["/docs/*.md"]);
        assert!(f.is_excluded("docs/a.md"));
        assert!(!f.is_excluded("docs/sub/a.md"));
    }
}
