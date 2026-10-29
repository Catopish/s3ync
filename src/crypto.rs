use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use age::secrecy::{ExposeSecret, SecretString};
use anyhow::{Context, Result, anyhow, bail};

use crate::config::Encryption;

/// Every age file starts with this line, so encrypted objects are recognized by content,
/// not by name or by whatever the config says today.
const MAGIC: &[u8] = b"age-encryption.org/v1\n";

type Recipient = Box<dyn age::Recipient + Send + Sync>;
type Identity = Box<dyn age::Identity + Send + Sync>;

/// Public keys that new objects are encrypted to.
pub struct Crypto {
    recipients: Vec<Recipient>,
}

impl Crypto {
    /// `None` when encryption is off. Without `recipients`, encrypts to the public key of
    /// `identity`, which must exist (see `s3ync keygen`).
    pub fn load(enc: &Encryption) -> Result<Option<Self>> {
        if !enc.enabled() {
            return Ok(None);
        }
        let mut recipients = Vec::new();
        for entry in &enc.recipients {
            recipients
                .extend(parse_recipients(entry).with_context(|| format!("recipient {entry:?}"))?);
        }
        if enc.recipients.is_empty() {
            let id = enc
                .identity
                .as_deref()
                .expect("validated: identity or recipients");
            recipients = recipients_of_identity(id)?;
        }
        Ok(Some(Self { recipients }))
    }

    pub fn recipient_count(&self) -> usize {
        self.recipients.len()
    }

    /// Wraps `out` so everything written is encrypted. `finish()` must be called.
    pub fn encrypt_to<W: Write>(&self, out: W) -> Result<age::stream::StreamWriter<W>> {
        let enc = age::Encryptor::with_recipients(self.recipients.iter().map(|r| r.as_ref() as _))?;
        Ok(enc.wrap_output(out)?)
    }

    pub fn encrypt_file(&self, src: &Path, dst: File) -> Result<()> {
        let mut input = File::open(src).with_context(|| format!("opening {}", src.display()))?;
        let mut w = self.encrypt_to(dst)?;
        io::copy(&mut input, &mut w).with_context(|| format!("encrypting {}", src.display()))?;
        w.finish()?.sync_all()?;
        Ok(())
    }
}

/// One `recipients` entry: an `age1...` key, an inline `ssh-...` public key, or a path to a
/// file with one key per line (an SSH `.pub` file, an age recipients file).
fn parse_recipients(entry: &str) -> Result<Vec<Recipient>> {
    if let Some(r) = parse_recipient(entry) {
        return r.map(|r| vec![r]);
    }
    let path = expand_tilde(entry);
    let raw =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let keys: Vec<Recipient> = raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| parse_recipient(l).unwrap_or_else(|| Err(anyhow!("not a public key: {l:?}"))))
        .collect::<Result<_>>()
        .with_context(|| format!("in {}", path.display()))?;
    if keys.is_empty() {
        bail!("{} contains no public keys", path.display());
    }
    Ok(keys)
}

/// `None` if `s` doesn't look like an inline key (so it's a path).
fn parse_recipient(s: &str) -> Option<Result<Recipient>> {
    if s.starts_with("age1") {
        Some(
            s.parse::<age::x25519::Recipient>()
                .map(|r| Box::new(r) as Recipient)
                .map_err(|e| anyhow!("invalid age public key: {e}")),
        )
    } else if s.starts_with("ssh-") {
        Some(
            s.parse::<age::ssh::Recipient>()
                .map(|r| Box::new(r) as Recipient)
                .map_err(|e| anyhow!("unsupported SSH public key: {e:?}")),
        )
    } else {
        None
    }
}

fn recipients_of_identity(path: &Path) -> Result<Vec<Recipient>> {
    if !path.exists() {
        bail!(
            "encryption identity {} does not exist; create it with `s3ync keygen` \
             or list public keys in encryption.recipients",
            path.display()
        );
    }
    let raw = std::fs::read_to_string(path).unwrap_or_default();
    let native: Vec<Recipient> = raw
        .lines()
        .filter_map(|l| l.trim().parse::<age::x25519::Identity>().ok())
        .map(|id| Box::new(id.to_public()) as Recipient)
        .collect();
    if !native.is_empty() {
        return Ok(native);
    }
    let id = read_ssh_identity(path)?;
    let r = age::ssh::Recipient::try_from(id)
        .map_err(|e| anyhow!("{}: cannot derive a public key: {e:?}", path.display()))?;
    Ok(vec![Box::new(r)])
}

/// Private keys for decryption: an age identity file (`AGE-SECRET-KEY-...`) or an SSH
/// private key. Passphrase-protected SSH keys ask on the terminal or read
/// `S3YNC_PASSPHRASE`.
pub fn load_identities(path: &Path) -> Result<Vec<Identity>> {
    let path = expand_tilde(&path.to_string_lossy());
    if let Ok(f) = age::IdentityFile::from_file(path.display().to_string()) {
        let ids = f
            .into_identities()
            .map_err(|e| anyhow!("{}: {e}", path.display()))?;
        if !ids.is_empty() {
            return Ok(ids.into_iter().map(|i| i as _).collect());
        }
    }
    let id = read_ssh_identity(&path)?;
    if let age::ssh::Identity::Unsupported(k) = &id {
        bail!("{}: unsupported SSH key: {k:?}", path.display());
    }
    Ok(vec![Box::new(id.with_callbacks(Prompt))])
}

fn read_ssh_identity(path: &Path) -> Result<age::ssh::Identity> {
    let f = File::open(path).with_context(|| format!("opening identity {}", path.display()))?;
    age::ssh::Identity::from_buffer(BufReader::new(f), Some(path.display().to_string()))
        .with_context(|| {
            format!(
                "{} is neither an age identity nor an SSH private key",
                path.display()
            )
        })
}

/// Asks for SSH key passphrases.
#[derive(Clone)]
struct Prompt;

impl age::Callbacks for Prompt {
    fn display_message(&self, message: &str) {
        eprintln!("{message}");
    }

    fn confirm(&self, _: &str, _: &str, _: Option<&str>) -> Option<bool> {
        None
    }

    fn request_public_string(&self, _: &str) -> Option<String> {
        None
    }

    fn request_passphrase(&self, description: &str) -> Option<SecretString> {
        if let Ok(p) = std::env::var("S3YNC_PASSPHRASE") {
            return Some(p.into());
        }
        rpassword::prompt_password(format!("{description}: "))
            .ok()
            .map(Into::into)
    }
}

/// Reads `input`, decrypting it if it is an age file. Returns whether it was encrypted.
pub fn maybe_decrypt<'a, R: Read + 'a>(
    input: R,
    identities: impl FnOnce() -> Result<&'a [Identity]>,
) -> Result<(Box<dyn Read + 'a>, bool)> {
    let mut input = BufReader::new(input);
    if !input.fill_buf()?.starts_with(MAGIC) {
        return Ok((Box::new(input), false));
    }
    let ids = identities()?;
    let dec = age::Decryptor::new_buffered(input)?;
    let r = dec
        .decrypt(ids.iter().map(|i| i.as_ref() as _))
        .map_err(|e| anyhow!("cannot decrypt: {e}"))?;
    Ok((Box::new(r), true))
}

/// Writes a new age identity to `path` (mode 0600, never overwrites); returns its public key.
pub fn keygen(path: &Path) -> Result<String> {
    use std::os::unix::fs::OpenOptionsExt;
    let key = age::x25519::Identity::generate();
    let public = key.to_public().to_string();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("creating {} (already exists?)", path.display()))?;
    writeln!(
        f,
        "# created: {}\n# public key: {public}\n{}",
        chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
        key.to_string().expose_secret()
    )?;
    f.sync_all()?;
    Ok(public)
}

/// `~/x` -> `$HOME/x`. Note that a root service's home is /root.
pub fn expand_tilde(p: &str) -> PathBuf {
    match (p.strip_prefix("~/"), std::env::var_os("HOME")) {
        (Some(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => PathBuf::from(p),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EncryptionType;

    fn enc(recipients: Vec<String>, identity: Option<PathBuf>) -> Encryption {
        Encryption {
            kind: EncryptionType::Age,
            recipients,
            identity,
        }
    }

    fn roundtrip(c: &Crypto, ids: &[Identity]) {
        let mut ct = Vec::new();
        let mut w = c.encrypt_to(&mut ct).unwrap();
        w.write_all(b"secret data").unwrap();
        w.finish().unwrap();
        assert!(ct.starts_with(MAGIC));
        let (mut r, was) = maybe_decrypt(&ct[..], || Ok(ids)).unwrap();
        let mut out = String::new();
        r.read_to_string(&mut out).unwrap();
        assert!(was);
        assert_eq!(out, "secret data");
    }

    #[test]
    fn keygen_identity_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("age.key");
        let public = keygen(&key).unwrap();
        assert!(public.starts_with("age1"));
        assert!(keygen(&key).is_err(), "must not overwrite");
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&key).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let ids = load_identities(&key).unwrap();
        // Recipients derived from the identity file...
        roundtrip(
            &Crypto::load(&enc(vec![], Some(key.clone())))
                .unwrap()
                .unwrap(),
            &ids,
        );
        // ...or given inline, plus a recipients file.
        let rfile = dir.path().join("recipients.txt");
        std::fs::write(&rfile, format!("# mine\n{public}\n")).unwrap();
        let c = Crypto::load(&enc(vec![public, rfile.display().to_string()], None))
            .unwrap()
            .unwrap();
        assert_eq!(c.recipient_count(), 2);
        roundtrip(&c, &ids);
    }

    #[test]
    fn ssh_ed25519_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("id_ed25519");
        let ok = std::process::Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(&key)
            .status()
            .is_ok_and(|s| s.success());
        if !ok {
            eprintln!("ssh-keygen not available, skipping");
            return;
        }
        let pubkey = format!("{}.pub", key.display());
        let c = Crypto::load(&enc(vec![pubkey], None)).unwrap().unwrap();
        roundtrip(&c, &load_identities(&key).unwrap());
        // Recipient derived from the private key when no recipients are listed.
        let c = Crypto::load(&enc(vec![], Some(key.clone())))
            .unwrap()
            .unwrap();
        roundtrip(&c, &load_identities(&key).unwrap());
    }

    #[test]
    fn plaintext_passes_through() {
        let (mut r, was) = maybe_decrypt(&b"plain"[..], || panic!("no key needed")).unwrap();
        let mut out = String::new();
        r.read_to_string(&mut out).unwrap();
        assert!(!was);
        assert_eq!(out, "plain");
    }

    #[test]
    fn wrong_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        keygen(&a).unwrap();
        keygen(&b).unwrap();
        let c = Crypto::load(&enc(vec![], Some(a))).unwrap().unwrap();
        let mut ct = Vec::new();
        let mut w = c.encrypt_to(&mut ct).unwrap();
        w.write_all(b"x").unwrap();
        w.finish().unwrap();
        let ids = load_identities(&b).unwrap();
        assert!(maybe_decrypt(&ct[..], || Ok(&ids[..])).is_err());
    }
}
