#!/usr/bin/env bash
# End-to-end test against a real S3-compatible bucket (CI uses Cloudflare R2).
#
#   S3YNC_BIN=target/release/s3ync S3YNC_TEST_BUCKET=my-bucket tests/e2e.sh
#
# Needs AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT_URL in the
# environment, plus the aws CLI (used to inspect the bucket and clean up).
# Everything is written under s3ync-e2e/<run id>/ and deleted at the end.
set -euo pipefail

BIN=$(realpath "${S3YNC_BIN:-target/release/s3ync}")
BUCKET=${S3YNC_TEST_BUCKET:?set S3YNC_TEST_BUCKET}
RUN=${S3YNC_TEST_RUN:-local-$(date +%s)}
ROOT="s3ync-e2e/$RUN"
W=$(mktemp -d)
SRC=$W/src
CFG=$W/config.toml
DAEMON_PID=

PASS=0
pass() { PASS=$((PASS + 1)); echo "  ok   $*"; }
fail() { echo "  FAIL $*" >&2; exit 1; }
step() { echo; echo "== $*"; }

cleanup() {
    [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null || true
    aws s3 rm "s3://$BUCKET/$ROOT/" --recursive --only-show-errors || true
    rm -rf "$W"
}
trap cleanup EXIT

# s3ync output without colors.
s3() { "$BIN" -c "$CFG" "$@" 2>&1 | sed $'s/\x1b\\[[0-9;]*m//g'; }
# Keys under a prefix, relative to it, sorted.
keys() {
    aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "$ROOT/$1" \
        --query 'Contents[].Key' --output text | tr '\t' '\n' | grep -v '^None$' |
        sed "s|^$ROOT/$1||" | sort || true
}
# Waits up to $2 seconds (default 30) for a command to succeed.
wait_for() {
    local cond=$1 t=${2:-30}
    for _ in $(seq $((t * 2))); do eval "$cond" && return 0; sleep 0.5; done
    fail "timed out after ${t}s waiting for: $cond"
}
has_key() { keys "$1" | grep -qx "$2"; }
log_has() { sed $'s/\x1b\\[[0-9;]*m//g' "$W/daemon.log" | grep -q "$1"; }
start_daemon() {
    "$BIN" -c "$CFG" run >"$W/daemon.log" 2>&1 &
    DAEMON_PID=$!
    wait_for 'log_has " ready "'
}
stop_daemon() {
    kill -TERM "$DAEMON_PID"
    local code=0
    wait "$DAEMON_PID" || code=$?
    DAEMON_PID=
    [ "$code" = 0 ] || fail "daemon exited with $code after SIGTERM"
}

mkdir -p "$W/state" "$SRC"/{plain/sub,plain/.git,enc,backup,backup-plain}
echo hello >"$SRC/plain/a.txt"
echo nested >"$SRC/plain/sub/b.txt"
echo tmp >"$SRC/plain/x.tmp"
echo git >"$SRC/plain/.git/HEAD"
echo "with space" >"$SRC/plain/sub/file name.txt"
echo secret >"$SRC/enc/secret.txt"
echo dump >"$SRC/backup/db.sql"
echo skip >"$SRC/backup/skip.tmp"
echo plain >"$SRC/backup-plain/data.txt"
ssh-keygen -q -t ed25519 -N "" -f "$W/ssh_key"

"$BIN" keygen -o "$W/age.key" >/dev/null 2>&1

cat >"$CFG" <<EOF
[global]
state_dir = "$W/state"
rescan_interval_secs = 20
multipart_threshold_mb = 8
part_size_mb = 5
force_path_style = true

[encryption]
type       = "age"
recipients = ["$(grep -o 'age1[0-9a-z]*' "$W/age.key" | head -1)", "$W/ssh_key.pub"]
identity   = "$W/age.key"

[[job]]
name    = "plain"
mode    = "sync"
bucket  = "$BUCKET"
source  = "$SRC/plain"
prefix  = "$ROOT/plain/"
delete  = true
exclude = ["*.tmp", ".git/"]
debounce_ms = 500
encrypt = false

[[job]]
name    = "enc"
mode    = "sync"
bucket  = "$BUCKET"
source  = "$SRC/enc"
prefix  = "$ROOT/enc/"
delete  = true
debounce_ms = 500

[[job]]
name      = "backup"
mode      = "backup"
bucket    = "$BUCKET"
source    = "$SRC/backup"
prefix    = "$ROOT/backups/"
exclude   = ["*.tmp"]
schedule  = "* * * * *"
naming    = "timestamp"
keep_last = 2

[[job]]
name    = "backup-plain"
mode    = "backup"
bucket  = "$BUCKET"
source  = "$SRC/backup-plain"
prefix  = "$ROOT/backups-plain/"
encrypt = false
EOF

step "check"
out=$(s3 check) || fail "check failed: $out"
[ "$(grep -c '^ok' <<<"$out")" = 5 ] && pass "config, encryption and 4 jobs ok" || fail "$out"

step "check reports the cause of failures"
out=$(AWS_SECRET_ACCESS_KEY=wrong-secret s3 check) && fail "check passed with a bad secret"
grep -q SignatureDoesNotMatch <<<"$out" && pass "bad secret -> SignatureDoesNotMatch" || fail "$out"
out=$(AWS_ENDPOINT_URL=https://s3ync-e2e.invalid s3 check) && fail "check passed with a bad endpoint"
grep -q "dns error" <<<"$out" && pass "bad endpoint -> dns error" || fail "$out"
sed "s/bucket  *= \"$BUCKET\"/bucket = \"s3ync-e2e-no-such-bucket-$RUN\"/" "$CFG" >"$W/bad.toml"
out=$("$BIN" -c "$W/bad.toml" check 2>&1) && fail "check passed with a missing bucket"
grep -qE "does not exist|AccessDenied" <<<"$out" && pass "missing bucket reported" || fail "$out"

step "initial sync"
start_daemon
wait_for 'has_key plain/ a.txt && has_key plain/ "sub/file name.txt" && has_key enc/ secret.txt.age'
[ "$(keys plain/)" = $'a.txt\nsub/b.txt\nsub/file name.txt' ] &&
    pass "plain files uploaded, *.tmp and .git/ excluded" || fail "plain keys: $(keys plain/)"
[ "$(keys enc/)" = "secret.txt.age" ] && pass "encrypted file stored as .age" || fail "enc keys: $(keys enc/)"
[ "$(aws s3 cp "s3://$BUCKET/$ROOT/plain/a.txt" -)" = hello ] && pass "plain object readable as-is" ||
    fail "a.txt content"
aws s3 cp "s3://$BUCKET/$ROOT/enc/secret.txt.age" "$W/secret.age" --only-show-errors
grep -q secret "$W/secret.age" && fail "encrypted object contains plaintext"
pass "encrypted object has no plaintext"

step "live changes"
echo new >"$SRC/plain/c.txt"
rm "$SRC/plain/a.txt"
echo changed >>"$SRC/enc/secret.txt"
mkdir -p "$SRC/plain/deep/er"
echo deep >"$SRC/plain/deep/er/d.txt"
head -c 20000000 /dev/urandom >"$SRC/plain/big.bin"
wait_for 'has_key plain/ c.txt && has_key plain/ deep/er/d.txt && has_key plain/ big.bin'
pass "new files and directories uploaded"
wait_for '! has_key plain/ a.txt'
pass "deleted file removed from bucket"
etag=$(aws s3api head-object --bucket "$BUCKET" --key "$ROOT/plain/big.bin" --query ETag --output text)
[[ $etag == *-* ]] && pass "20 MB file used multipart upload" || fail "big.bin etag $etag is not multipart"
mv "$SRC/plain/c.txt" "$SRC/plain/renamed.txt"
wait_for 'has_key plain/ renamed.txt && ! has_key plain/ c.txt'
pass "rename uploads new key and deletes old"

step "rescan catches changes made while stopped"
stop_daemon
pass "SIGTERM exits 0"
echo offline >"$SRC/plain/offline.txt"
rm "$SRC/plain/sub/b.txt"
start_daemon
wait_for 'has_key plain/ offline.txt && ! has_key plain/ sub/b.txt'
pass "offline add and delete reconciled on startup"
stop_daemon
start_daemon
sleep 3
sed $'s/\x1b\\[[0-9;]*m//g' "$W/daemon.log" | grep -E "s3ync::sync: (uploaded|deleted) " && fail "restart re-uploaded files"
pass "restart with no changes uploads nothing"
echo restored-later >"$SRC/plain/late.txt"
wait_for 'has_key plain/ late.txt'
pass "watcher works after restart"

step "scheduled backup"
echo scheduled >>"$SRC/backup/db.sql"
wait_for 'log_has "backup uploaded.*job=backup"' 75
pass "daemon took the scheduled backup"
stop_daemon

step "manual backups"
out=$(s3 backup --job backup) && grep -q "(unchanged)" <<<"$out" && pass "unchanged backup not uploaded" || fail "$out"
for i in 1 2 3; do
    echo "v$i" >>"$SRC/backup/db.sql"
    sleep 1.1 # timestamped keys have second resolution
    out=$(s3 backup --job backup) && grep -q "(uploaded)" <<<"$out" || fail "backup $i: $out"
done
pass "changed backups uploaded"
grep -q "deleted 1 old backup" <<<"$out" && pass "keep_last pruned old backups" || fail "$out"
[ "$(keys backups/ | wc -l | tr -d ' ')" = 2 ] && pass "2 backups kept" || fail "backups: $(keys backups/)"
keys backups/ | grep -qv '\.tar\.zst\.age$' && fail "backup not encrypted: $(keys backups/)"
pass "backups are .tar.zst.age"
[ "$(s3 restore --job backup --list | grep -c encrypted)" = 2 ] && pass "restore --list shows 2" || fail "list"
out=$(s3 backup --job backup-plain) && grep -q "(uploaded)" <<<"$out" || fail "$out"
[ "$(keys backups-plain/)" = "backup-plain.tar.zst" ] && pass "unencrypted fixed-name backup" ||
    fail "$(keys backups-plain/)"
out=$(s3 backup) && pass "backup without --job runs all backup jobs" || fail "$out"
out=$(s3 status) && grep -q "^plain" <<<"$out" && pass "status" || fail "$out"

step "restore"
R=$W/restore
s3 restore --job backup --to "$R/backup" >/dev/null
diff -r --exclude=skip.tmp "$R/backup" "$SRC/backup" && pass "latest encrypted backup restores identical" ||
    fail "backup diff"
s3 restore --job backup --to "$R/backup-at" --at "$(date -u +%Y-%m-%d)" >/dev/null &&
    [ -f "$R/backup-at/db.sql" ] && pass "restore --at" || fail "restore --at"
s3 restore --job backup-plain --to "$R/backup-plain" >/dev/null
diff -r "$R/backup-plain" "$SRC/backup-plain" && pass "unencrypted backup restores (hash checked)" ||
    fail "backup-plain diff"
s3 restore --job plain --to "$R/plain" >/dev/null
diff -r --exclude=x.tmp --exclude=.git "$R/plain" "$SRC/plain" && pass "sync job restores identical" ||
    fail "plain diff"
s3 restore --job enc --to "$R/enc" >/dev/null
diff -r "$R/enc" "$SRC/enc" && pass "encrypted sync job restores with age identity" || fail "enc diff"
s3 restore --job enc --to "$R/enc-ssh" -i "$W/ssh_key" >/dev/null
diff -r "$R/enc-ssh" "$SRC/enc" && pass "decrypts with ssh-ed25519 recipient key" || fail "enc ssh diff"
s3 restore --job plain --to "$R/sub" --path deep >/dev/null
[ "$(cd "$R/sub" && find . -type f)" = "./deep/er/d.txt" ] && pass "restore --path" || fail "--path"
out=$(s3 restore --job plain --to "$R/plain") && grep -q " 0 failed" <<<"$out" &&
    grep -qE "restored 0 files" <<<"$out" && pass "existing files skipped without --force" || fail "$out"
echo local-edit >"$R/plain/late.txt"
s3 restore --job plain --to "$R/plain" --force >/dev/null
grep -qx restored-later "$R/plain/late.txt" && pass "--force overwrites" || fail "--force"
out=$(s3 restore --job backup --to "$R/backup") && fail "restored into non-empty dir"
grep -q "not empty" <<<"$out" && pass "backup into non-empty dir refused" || fail "$out"
"$BIN" keygen -o "$W/wrong.key" >/dev/null 2>&1
out=$(s3 restore --job enc --to "$R/wrong" -i "$W/wrong.key") && fail "restored with wrong key"
grep -q "No matching keys" <<<"$out" && pass "wrong key rejected" || fail "$out"

step "backup-only daemon"
# Last: it rewrites status.json and takes a catch-up backup.
# The config up to the first [[job]], plus only the scheduled backup job.
{
    sed '/^\[\[job\]\]/,$d' "$CFG"
    awk '/^\[\[job\]\]/{n++} n==3' "$CFG"
} >"$W/backup-only.toml"
"$BIN" -c "$W/backup-only.toml" run >"$W/daemon.log" 2>&1 &
DAEMON_PID=$!
wait_for 'log_has " ready "' 20
pass "daemon starts with no sync jobs"
stop_daemon

echo
echo "all $PASS checks passed"
