# s3ync

A small daemon that keeps local directories in sync with an S3 bucket.
It watches for changes (inotify) and uploads them within seconds, and does a full
reconcile on startup and periodically so nothing is missed while it was down.

- One-way: local → S3. Objects that exist only in S3 are never touched.
- Files are stored as plain objects (`<prefix>/<relative path>`), browsable in the console.
- Restarts are cheap: a local state DB remembers what was uploaded.
- Works with AWS S3 and S3-compatible stores (MinIO, SeaweedFS, R2, ...).

> Status: live sync works. Scheduled backups and client-side encryption are planned.

## Install

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
synced directories.

## Configuration

`.env` holds credentials using the standard AWS variable names:

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
# AWS_SESSION_TOKEN=...
# AWS_ENDPOINT_URL=https://...   # S3-compatible stores; also set force_path_style = true
```

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

Exclude patterns are gitignore-like: `*.tmp` matches at any depth, `dir/` matches a
directory and everything in it, a pattern containing `/` (like `build/out`) is anchored
to `source`.

### The bucket

s3ync never creates buckets; create it yourself. Recommended:

- Enable **versioning**, so an accidental overwrite or delete can be undone.
- Add a lifecycle rule to **abort incomplete multipart uploads** after a day or so
  (left behind if s3ync is killed mid-upload).

Minimal IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::my-bucket" },
    { "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-bucket/*" }
  ]
}
```

## Commands

| Command | |
|---|---|
| `s3ync run` | Daemon: watch and sync until SIGTERM |
| `s3ync check` | Validate config, check each bucket is reachable and listable |
| `s3ync once [--job NAME]` | One reconcile pass, then exit (non-zero if anything failed) |
| `s3ync status` | Per-job counters from the running daemon |

Global flags: `-c/--config` (default `/etc/s3ync/config.toml`, env `S3YNC_CONFIG`),
`--env` (default `./.env` if present, env `S3YNC_ENV`). Log level: `S3YNC_LOG=s3ync=debug`.

## How it works

- **Change detection**: size + mtime as a fast check, BLAKE3 hash to decide whether
  content actually changed. The hash is stored in object metadata (`x-amz-meta-s3ync-hash`),
  so with a fresh state DB existing objects are verified with a HEAD instead of re-uploaded.
- **Watching**: events are debounced (`debounce_ms`), then queued; uploads run with
  bounded concurrency and retry with exponential backoff (up to 5 min).
- **Torn writes**: if a file changes while it's being uploaded, the upload is retried.
- **Deletes**: only for paths s3ync uploaded itself, and only with `delete = true`.
- **Large trees**: each directory uses an inotify watch. If you hit the limit, raise it:
  `echo fs.inotify.max_user_watches=524288 | sudo tee /etc/sysctl.d/90-s3ync.conf && sudo sysctl --system`.

## License

GPL-3.0
