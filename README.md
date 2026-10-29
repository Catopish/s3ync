# s3ync

A small daemon that keeps local directories in sync with an S3 bucket.
It watches for changes (inotify) and uploads them within seconds, and does a full
reconcile on startup and periodically so nothing is missed while it was down.
It can also take scheduled backups: a directory packed into one `.tar.zst` archive.
Both are configured in one `config.toml` and run by the same daemon.

- One-way: local → S3. Objects that exist only in S3 are never touched.
- Files are stored as plain objects (`<prefix>/<relative path>`), browsable in the console,
  or encrypted client-side with [age](https://age-encryption.org) (`<relative path>.age`).
- Restarts are cheap: a local state DB remembers what was uploaded.
- Works with AWS S3 and S3-compatible stores (MinIO, SeaweedFS, R2, ...).

> Status: live sync, scheduled backups, age encryption and restore work.

## Install

s3ync runs the same way everywhere: `s3ync run` is the daemon (live sync + backups on
their `schedule`), `s3ync backup` takes backups once and exits. Pick how to host it:

### systemd

```sh
cargo build --release
sudo install -m755 target/release/s3ync /usr/local/bin/
sudo mkdir -p /etc/s3ync
sudo cp dist/config.example.toml /etc/s3ync/config.toml
sudo install -m600 dist/env.example /etc/s3ync/.env
sudo cp dist/s3ync.service /etc/systemd/system/
```

Edit `/etc/s3ync/config.toml` and `/etc/s3ync/.env`, then:

```sh
s3ync -c /etc/s3ync/config.toml --env /etc/s3ync/.env check   # validate + test bucket access
sudo systemctl enable --now s3ync
s3ync -c /etc/s3ync/config.toml status
```

The service runs as root by default; set `User=` in the unit to the user that owns the
synced directories. Schedules use the host's time zone.

### Docker

```sh
docker compose -f dist/compose.yaml up -d --build
docker compose -f dist/compose.yaml exec s3ync s3ync status
```

Edit [`dist/compose.yaml`](dist/compose.yaml) first: it mounts `/etc/s3ync/config.toml`,
reads credentials from `/etc/s3ync/.env`, and needs every job's `source` mounted at the
same path as in the config. Set `TZ` for schedules (default UTC). On Docker Desktop/colima
file events from the host may not reach the container; `rescan_interval_secs` still
catches those changes, just later.

### One-off backups

Without a daemon, run backups when you like (a manual run, cron, a CI job, ...):

```sh
s3ync -c /etc/s3ync/config.toml --env /etc/s3ync/.env backup             # all backup jobs
s3ync -c /etc/s3ync/config.toml --env /etc/s3ync/.env backup --job db-dumps
docker compose -f dist/compose.yaml run --rm s3ync backup --job db-dumps
```

## Configuration

`.env` holds credentials using the standard AWS variable names:

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
# AWS_SESSION_TOKEN=...
# AWS_ENDPOINT_URL=https://...   # S3-compatible stores; also set force_path_style = true
```

For **Cloudflare R2** (free tier: 10 GB-month storage, 1M writes, 10M reads, free egress),
create an API token with "Object Read & Write" for the bucket and use:

```env
AWS_ACCESS_KEY_ID=<token access key id>
AWS_SECRET_ACCESS_KEY=<token secret>
AWS_REGION=auto
AWS_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
```

R2 has no bucket versioning, so give backup jobs `naming = "timestamp"` (see below).

`config.toml` defines one or more jobs. See [`dist/config.example.toml`](dist/config.example.toml)
for all options.

```toml
[[job]]
name    = "photos"
mode    = "sync"
bucket  = "my-bucket"
source  = "/home/me/photos"
prefix  = "photos/"
delete  = false
exclude = ["*.tmp", ".git/"]
```

`mode = "sync"` jobs are watched by the daemon. `mode = "backup"` jobs are taken by the
daemon on their `schedule` (cron syntax, local time), and by `s3ync backup` at any time;
without a `schedule` only the latter. `delete` is not allowed for them:

```toml
[[job]]
name    = "db-dumps"
mode    = "backup"
bucket  = "my-bucket"
source  = "/var/backups/db"
prefix  = "backups/"     # -> s3://my-bucket/backups/db-dumps.tar.zst
exclude = ["*.tmp"]
schedule = "0 2 * * *"   # daily at 02:00
```

If the daemon was down at a scheduled time, the missed backup runs as soon as it starts
again (the last successful run per job is kept in `state_dir/backups.json`). A failed
backup is retried with backoff (1 min doubling up to 1 h) until the next scheduled time.

By default every run overwrites that one key and older backups live on as object
versions. For buckets without versioning (Cloudflare R2, some self-hosted stores) use
timestamped keys instead; s3ync then keeps the newest `keep_last` (0 = all) and deletes
older ones after each run:

```toml
naming    = "timestamp"  # -> s3://my-bucket/backups/db-dumps/db-dumps-2026-10-29T02-00-00Z.tar.zst
keep_last = 14
```

An unchanged run never adds a new key, so `keep_last` counts distinct backups, and the
newest one is never deleted however old it is. Other objects in that folder are left alone.

Exclude patterns are gitignore-like: `*.tmp` matches at any depth, `dir/` matches a
directory and everything in it, a pattern containing `/` (like `build/out`) is anchored
to `source`.

### Encryption

With an `[encryption]` section every job's objects are encrypted with age before they
leave the machine (a job can opt out with `encrypt = false`). Synced files are stored as
`<path>.age`, backups as `.tar.zst.age`. Use your SSH key, an age key, or several:

```toml
[encryption]
type       = "age"
recipients = ["~/.ssh/id_ed25519.pub", "age1..."]   # public keys (or files holding them)
identity   = "/etc/s3ync/age.key"                     # private key, used by restore
```

With no `recipients`, data is encrypted to the public key of `identity`. That key is
never created implicitly; make it with:

```sh
s3ync keygen                  # writes encryption.identity (0600), prints the public key
s3ync keygen -o spare.key     # another key, e.g. a spare to add to recipients
```

**Keep a copy of the private key off the machine** (password manager, offline drive):
if the disk dies, the key goes with it, and the backups can't be decrypted. Listing a
second recipient whose private key lives elsewhere is the easiest insurance. The daemon
only needs public keys, so the private key need not be on the server at all when
`recipients` is set. `~` means the home of the user s3ync runs as (`/root` for the
default service).

What stays visible to the storage provider: bucket/key names (so file paths and sizes,
roughly), timestamps. Encrypted objects carry no content hash. Because of that, an
encrypted sync job with a fresh state DB re-uploads everything once, and an encrypted
backup's "unchanged" check relies on `state_dir/backups.json`.

Enabling encryption on an existing job uploads everything again under the new `.age`
keys; the old plaintext objects are left in the bucket for you to delete.

### Restore

`s3ync restore` works from the config alone, so it also runs on a fresh machine where the
jobs' `source` directories don't exist. Encrypted objects are recognized by content and
decrypted with `--identity` (an age key or SSH private key; default
`encryption.identity`). Passphrase-protected SSH keys are prompted for, or read from
`S3YNC_PASSPHRASE`.

```sh
s3ync restore --job db-dumps --list                          # backups available
s3ync restore --job db-dumps --to /tmp/db                    # the latest
s3ync restore --job db-dumps --to /tmp/db --at 2026-10-29    # last one of that day
s3ync restore --job db-dumps --to /tmp/db --version-id ID    # naming = "fixed", versioned bucket
s3ync restore --job photos --to /tmp/photos -i ~/.ssh/id_ed25519
s3ync restore --job photos --to /tmp/photos --path 2024/summer
```

The target must be empty (backups) or existing files are skipped (sync jobs), unless
`--force`. Unencrypted backups are checked against their stored hash; age protects the
integrity of encrypted ones. Paths that would escape the target (`..`, absolute) are
refused.

### The bucket

s3ync never creates buckets; create it yourself. Recommended:

- Enable **versioning**, so an accidental overwrite or delete can be undone. Backup jobs
  rely on it: every run overwrites the same key, older backups are the previous versions.
- For backups, add a lifecycle rule that **expires noncurrent versions** to control how
  many old backups you keep (e.g. "keep 7 noncurrent versions" or "delete after 30 days").
  s3ync never deletes these itself. (Age-based expiry is not safe with
  `naming = "timestamp"`: if nothing changes for a while, it would delete the only
  backup. Use `keep_last` there.)
- Add a lifecycle rule to **abort incomplete multipart uploads** after a day or so
  (left behind if s3ync is killed mid-upload).

Minimal IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::my-bucket" },
    { "Effect": "Allow", "Action": ["s3:GetBucketVersioning", "s3:ListBucketVersions"],
      "Resource": "arn:aws:s3:::my-bucket" },
    { "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-bucket/*" }
  ]
}
```

## Commands

| Command | |
|---|---|
| `s3ync run` | Daemon: live sync and scheduled backups until SIGTERM |
| `s3ync check` | Validate config, check each bucket is reachable and listable |
| `s3ync once [--job NAME]` | One reconcile pass, then exit (non-zero if anything failed) |
| `s3ync status` | Per-job counters from the running daemon (last/next backup too) |
| `s3ync backup [--job NAME]` | Archive and upload backup jobs, then exit (non-zero if any failed) |
| `s3ync restore --job NAME --to DIR` | Restore a backup or a sync job's files (see above) |
| `s3ync keygen [-o FILE]` | Create an age key pair for `[encryption]` |

`s3ync --help` and `s3ync <command> --help` describe every flag.

Global flags: `-c/--config` (default `/etc/s3ync/config.toml`, env `S3YNC_CONFIG`),
`--env` (default `./.env` if present, env `S3YNC_ENV`). Log level: `S3YNC_LOG=s3ync=debug`.

## How it works

- **Change detection**: size + mtime as a fast check, BLAKE3 hash to decide whether
  content actually changed. The hash is stored in object metadata (`x-amz-meta-s3ync-hash`,
  unencrypted objects only), so with a fresh state DB existing objects are verified with
  a HEAD instead of re-uploaded.
- **Encryption**: files are encrypted into a temp file under `state_dir/tmp`, then
  uploaded; backups are encrypted as they are written (tar → zstd → age).
- **Watching**: events are debounced (`debounce_ms`), then queued; uploads run with
  bounded concurrency and retry with exponential backoff (up to 5 min).
- **Torn writes**: if a file changes while it's being uploaded, the upload is retried.
- **Deletes**: only for paths s3ync uploaded itself, and only with `delete = true`.
- **Backups**: files are packed in sorted order into a tar stream, compressed with zstd
  into a temp file under `state_dir/tmp` (needs free space for one archive), then
  uploaded. The BLAKE3 hash of the tar stream is stored on the object; if it matches
  the current object, nothing is uploaded, so unchanged runs don't add versions.
  Unreadable files are skipped with a warning and make the run exit non-zero; files
  changing mid-backup are archived as-is with a warning. Symlinks and empty
  directories are not included (unless `follow_symlinks = true`).
- **Large trees**: each directory uses an inotify watch. If you hit the limit, raise it:
  `echo fs.inotify.max_user_watches=524288 | sudo tee /etc/sysctl.d/90-s3ync.conf && sudo sysctl --system`.

## License

GPL-3.0
