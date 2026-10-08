#!/usr/bin/env bash
# scripts/tests/test_versebus.sh
# Minimal assertion-based tests for scripts/versebus.sh
# (ECOSYSTEM-FIX-PLAN.md Section 4 pannadata test list). No `bats`
# dependency -- plain bash + exit-code assertions, matching the repo's
# existing lightweight test conventions (pytest covers the Python scraper;
# this is the bash-uploader equivalent). Mocks `gh` as a shell function --
# bash resolves functions before PATH binaries in the same shell -- so no
# network is hit.
#
# Run: bash scripts/tests/test_versebus.sh

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../versebus.sh"

pass_count=0
fail_count=0

pass() { echo "PASS: $1"; pass_count=$((pass_count + 1)); }
fail() { echo "FAIL: $1"; fail_count=$((fail_count + 1)); }

check() {
  # check <desc> <command...> -- PASS iff the command exits 0.
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}

check_not() {
  # check_not <desc> <command...> -- PASS iff the command exits non-zero.
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then fail "$desc"; else pass "$desc"; fi
}

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
echo "dummy content" > "$tmpdir/a.parquet"

# No real waiting in tests (safe upload / restore back off between retries).
sleep() { :; }
# Windows jq emits CRLF, which breaks the size comparisons; Linux CI is
# unaffected. pipefail keeps jq's own exit status.
jq() { command jq "$@" | tr -d '\r'; }

# ---------------------------------------------------------------------------
# Fake release: a stateful `gh` that keeps assets as real files plus a JSON
# listing, so the upload -> list -> delete -> rename and restore paths run
# for real. Failure switches (set to 1 to trigger):
#   FAKE_UPLOAD_FAIL FAKE_LIST_FAIL FAKE_PATCH_FAIL FAKE_DELETE_FAIL
#   FAKE_DOWNLOAD_FAIL FAKE_DOWNLOAD_TRUNC FAKE_UPLOAD_STUCK (upload lands
#   in state "starter", i.e. never completes)
# ---------------------------------------------------------------------------
fake_reset() {
  FAKE_DIR="$tmpdir/fake_release_$RANDOM$RANDOM"
  mkdir -p "$FAKE_DIR/files"
  echo '[]' > "$FAKE_DIR/assets.json"
  FAKE_NEXT_ID=1
  FAKE_UPLOAD_FAIL=0 FAKE_LIST_FAIL=0 FAKE_PATCH_FAIL=0 FAKE_DELETE_FAIL=0
  FAKE_DOWNLOAD_FAIL=0 FAKE_DOWNLOAD_TRUNC=0 FAKE_UPLOAD_STUCK=0
}
fake_put() {  # fake_put <name> <content> -- seed an asset directly
  printf '%s' "$2" > "$FAKE_DIR/files/$1"
  _fake_add "$1" "$(stat -c%s "$FAKE_DIR/files/$1")" uploaded
}
_fake_add() {
  local tmp="$FAKE_DIR/assets.tmp"
  command jq --arg n "$1" --argjson s "$2" --arg st "$3" --argjson id "$FAKE_NEXT_ID" \
    '. + [{id: $id, name: $n, size: $s, state: $st, created_at: ("2026-10-08T00:00:" + ($id | tostring | if length < 2 then "0" + . else . end) + "Z")}]' \
    "$FAKE_DIR/assets.json" > "$tmp" && mv "$tmp" "$FAKE_DIR/assets.json"
  FAKE_NEXT_ID=$((FAKE_NEXT_ID + 1))
}
fake_names() { command jq -r '[.[].name] | sort | join(" ")' "$FAKE_DIR/assets.json" | tr -d '\r'; }
fake_content() { cat "$FAKE_DIR/files/$1" 2>/dev/null; }

gh() {
  local tmp="$FAKE_DIR/assets.tmp"
  if [ "$1" = "api" ] && [ "$2" = "-X" ]; then
    local method="$3" id="${4##*/}"
    local name
    name=$(command jq -r --argjson id "$id" '.[] | select(.id == $id) | .name' "$FAKE_DIR/assets.json" | tr -d '\r')
    [ -n "$name" ] || return 1
    if [ "$method" = "DELETE" ]; then
      [ "$FAKE_DELETE_FAIL" = 1 ] && return 1
      rm -f "$FAKE_DIR/files/$name"
      command jq --argjson id "$id" 'map(select(.id != $id))' "$FAKE_DIR/assets.json" > "$tmp" && mv "$tmp" "$FAKE_DIR/assets.json"
      return 0
    elif [ "$method" = "PATCH" ]; then
      [ "$FAKE_PATCH_FAIL" = 1 ] && return 1
      local new="${6#name=}"
      mv "$FAKE_DIR/files/$name" "$FAKE_DIR/files/$new"
      command jq --argjson id "$id" --arg n "$new" 'map(if .id == $id then .name = $n else . end)' "$FAKE_DIR/assets.json" > "$tmp" && mv "$tmp" "$FAKE_DIR/assets.json"
      echo '{}'
      return 0
    fi
    return 1
  elif [ "$1" = "api" ]; then
    [ "$FAKE_LIST_FAIL" = 1 ] && return 1
    cat "$FAKE_DIR/assets.json"
    return 0
  elif [ "$1" = "release" ] && [ "$2" = "upload" ]; then
    [ "$FAKE_UPLOAD_FAIL" = 1 ] && return 1
    local f="$4" name
    name=$(basename "$f")
    cp "$f" "$FAKE_DIR/files/$name"
    if [ "$FAKE_UPLOAD_STUCK" = 1 ]; then
      _fake_add "$name" "$(stat -c%s "$f")" starter
    else
      _fake_add "$name" "$(stat -c%s "$f")" uploaded
    fi
    return 0
  elif [ "$1" = "release" ] && [ "$2" = "download" ]; then
    [ "$FAKE_DOWNLOAD_FAIL" = 1 ] && return 1
    local pattern="" dir="" prev=""
    for a in "$@"; do
      [ "$prev" = "--pattern" ] && pattern="$a"
      [ "$prev" = "--dir" ] && dir="$a"
      prev="$a"
    done
    [ -f "$FAKE_DIR/files/$pattern" ] || return 1
    if [ "$FAKE_DOWNLOAD_TRUNC" = 1 ]; then
      head -c 1 "$FAKE_DIR/files/$pattern" > "$dir/$pattern"
    else
      cp "$FAKE_DIR/files/$pattern" "$dir/$pattern"
    fi
    return 0
  fi
  echo "unexpected gh invocation: $*" >&2
  return 1
}
# Keep a copy so a section can swap in a one-off gh and switch back.
eval "$(declare -f gh | sed '1s/^gh /gh_fake /')"
fake_reset

# ---------------------------------------------------------------------------
# 1. vb_sh_manifest_last refuses when upload_errors != 0 -- no manifest file
#    is written, and it returns non-zero.
# ---------------------------------------------------------------------------
out_refused="$tmpdir/refused_bus_manifest.json"
check_not "vb_sh_manifest_last returns non-zero when upload_errors=1" \
  vb_sh_manifest_last "test/fixture" "test-tag" 1 "$out_refused" "$tmpdir/a.parquet"
check_not "vb_sh_manifest_last does NOT write a manifest file when upload_errors=1" \
  test -f "$out_refused"

# ---------------------------------------------------------------------------
# 2. vb_sh_manifest_last produces a §1.2-schema-valid manifest when
#    upload_errors=0 (first-ever publish -- no previous manifest on the tag).
# ---------------------------------------------------------------------------
fake_reset  # empty release -- simulates a first-ever publish

out_fresh="$tmpdir/fresh_bus_manifest.json"
check "vb_sh_manifest_last succeeds when upload_errors=0" \
  vb_sh_manifest_last "test/fixture" "test-tag" 0 "$out_fresh" "$tmpdir/a.parquet"
check "vb_sh_manifest_last writes a manifest file" test -f "$out_fresh"

if [ -f "$out_fresh" ]; then
  check "manifest schema_version == 1" jq -e '.schema_version == 1' "$out_fresh"
  check "manifest tag == test-tag" jq -e '.tag == "test-tag"' "$out_fresh"
  check "manifest has a non-empty generation string" \
    jq -e '(.generation | type) == "string" and (.generation | length) > 0' "$out_fresh"
  check "manifest has produced_at_utc string" jq -e '(.produced_at_utc | type) == "string"' "$out_fresh"
  check "manifest has a producer object" jq -e '(.producer | type) == "object"' "$out_fresh"
  check "manifest has exactly one asset entry" jq -e '(.assets | length) == 1' "$out_fresh"
  check "asset name == a.parquet" jq -e '.assets[0].name == "a.parquet"' "$out_fresh"
  check "asset sha256 is 64 lowercase hex chars" \
    jq -e '.assets[0].sha256 | test("^[0-9a-f]{64}$")' "$out_fresh"
  want_bytes=$(stat -c%s "$tmpdir/a.parquet")
  check "asset bytes matches local file size" \
    jq -e --argjson want "$want_bytes" '.assets[0].bytes == $want' "$out_fresh"
fi

# ---------------------------------------------------------------------------
# 3. Carry-forward: an asset present in the PREVIOUS manifest but not
#    re-uploaded this run survives in the merged manifest (partial-tag
#    publish, e.g. per-league events_*.parquet files the mtime-skip logic
#    left untouched).
# ---------------------------------------------------------------------------
prev_dir="$tmpdir/prev_dl"
mkdir -p "$prev_dir"
old_sha=$(printf 'b%.0s' $(seq 1 64))
cat > "$prev_dir/bus_manifest.json" <<EOF
{"schema_version":1,"tag":"test-tag","generation":"old-gen","produced_at_utc":"2020-01-01T00:00:00Z",
 "producer":{"repo":"x","workflow":"x","run_id":"","run_attempt":""},
 "assets":[{"name":"old_only.parquet","sha256":"$old_sha","bytes":5,"rows":null}],
 "notes":""}
EOF

fake_reset
fake_put bus_manifest.json "$(cat "$prev_dir/bus_manifest.json")"

out_carry="$tmpdir/carry_bus_manifest.json"
check "vb_sh_manifest_last succeeds with a previous manifest present" \
  vb_sh_manifest_last "test/fixture" "test-tag" 0 "$out_carry" "$tmpdir/a.parquet"

if [ -f "$out_carry" ]; then
  check "carry-forward: this run's new file is present" \
    jq -e '[.assets[].name] | index("a.parquet") != null' "$out_carry"
  check "carry-forward: previous-run-only file is carried forward" \
    jq -e '[.assets[].name] | index("old_only.parquet") != null' "$out_carry"
  check "carry-forward: exactly 2 assets (no duplicates)" \
    jq -e '(.assets | length) == 2' "$out_carry"
fi

# ---------------------------------------------------------------------------
# 3b. Canonical asset name: the release asset must be uploaded as
#     bus_manifest.json even when out_manifest_path has a different basename
#     (gh names assets by file basename; a models_manifest.json out_path
#     shipped under the wrong asset name 2026-07-17 before this guard).
# ---------------------------------------------------------------------------
fake_reset  # no previous manifest

out_oddname="$tmpdir/models_manifest.json"
check "vb_sh_manifest_last succeeds with a non-canonical out_path basename" \
  vb_sh_manifest_last "test/fixture" "test-tag" 0 "$out_oddname" "$tmpdir/a.parquet"
check "non-canonical out_path still writes the caller's local copy" \
  test -f "$out_oddname"
if [ "$(fake_names)" = "bus_manifest.json" ]; then
  pass "uploaded asset name is bus_manifest.json regardless of out_path"
else
  fail "release holds '$(fake_names)', expected exactly bus_manifest.json"
fi

# 3c. A previous manifest that is listed but fails to download must NOT be
#     treated as a first publish (the merge would drop every carried entry).
fake_reset
fake_put bus_manifest.json "$(cat "$prev_dir/bus_manifest.json")"
FAKE_DOWNLOAD_FAIL=1
out_dlfail="$tmpdir/dlfail_bus_manifest.json"
check_not "vb_sh_manifest_last refuses when the previous manifest can't be downloaded" \
  vb_sh_manifest_last "test/fixture" "test-tag" 0 "$out_dlfail" "$tmpdir/a.parquet"
if [ "$(fake_content bus_manifest.json)" = "$(cat "$prev_dir/bus_manifest.json")" ]; then
  pass "previous manifest left untouched after a failed read"
else
  fail "previous manifest was replaced after a failed read"
fi

# ---------------------------------------------------------------------------
# 4. YAML ordering regression guard (grep-based, no yaml lib dependency):
#    daily-opta-scrape.yml must upload opta-manifest.parquet AFTER the
#    opta_*.parquet loop, and bus_manifest.json's own call must come after
#    that -- guards against a future edit silently regressing panna C1
#    (manifest-first, ungated).
# ---------------------------------------------------------------------------
workflow="$SCRIPT_DIR/../../.github/workflows/daily-opta-scrape.yml"
if [ -f "$workflow" ]; then
  parquet_loop_line=$(grep -n 'for f in opta/opta_\*\.parquet' "$workflow" | head -1 | cut -d: -f1)
  domain_manifest_line=$(grep -n 'vb_sh_safe_upload "peteowen1/pannadata" opta-latest opta-manifest\.parquet' "$workflow" | head -1 | cut -d: -f1)
  bus_manifest_line=$(grep -n 'vb_sh_manifest_last "peteowen1/pannadata" "opta-latest"' "$workflow" | head -1 | cut -d: -f1)
  gate_line=$(grep -n 'if \[ "\$upload_errors" -eq 0 \]' "$workflow" | head -1 | cut -d: -f1)

  if [ -n "$parquet_loop_line" ] && [ -n "$domain_manifest_line" ]; then
    if [ "$domain_manifest_line" -gt "$parquet_loop_line" ]; then
      pass "daily-opta-scrape.yml: opta-manifest.parquet upload is AFTER the opta_*.parquet loop"
    else
      fail "daily-opta-scrape.yml: opta-manifest.parquet upload is NOT after the opta_*.parquet loop"
    fi
  else
    fail "daily-opta-scrape.yml: could not locate the parquet loop / domain-manifest upload lines"
  fi

  if [ -n "$domain_manifest_line" ] && [ -n "$bus_manifest_line" ]; then
    if [ "$bus_manifest_line" -gt "$domain_manifest_line" ]; then
      pass "daily-opta-scrape.yml: bus_manifest.json publish is AFTER opta-manifest.parquet upload"
    else
      fail "daily-opta-scrape.yml: bus_manifest.json publish is NOT after opta-manifest.parquet upload"
    fi
  else
    fail "daily-opta-scrape.yml: could not locate the bus_manifest.json publish call"
  fi

  if [ -n "$gate_line" ] && [ -n "$domain_manifest_line" ] && [ -n "$bus_manifest_line" ]; then
    if [ "$domain_manifest_line" -gt "$gate_line" ] && [ "$bus_manifest_line" -gt "$gate_line" ]; then
      pass "daily-opta-scrape.yml: both manifest uploads are inside the upload_errors -eq 0 gate"
    else
      fail "daily-opta-scrape.yml: a manifest upload appears OUTSIDE the upload_errors -eq 0 gate"
    fi
  else
    fail "daily-opta-scrape.yml: could not locate the upload_errors -eq 0 gate"
  fi
else
  fail "daily-opta-scrape.yml not found at $workflow"
fi

# ---------------------------------------------------------------------------
# 5. R2 trio (P5-A). wrangler is mocked as a shell function, same fixture
#    style as the gh mock.
# ---------------------------------------------------------------------------
echo "more dummy content" > "$tmpdir/b.parquet"
wrangler_log="$tmpdir/wrangler_invocations"
: > "$wrangler_log"

# 5a. vb_sh_r2_upload_all is FAIL-FAST: b.parquet upload fails -> return 1,
#     and a.parquet (later in the list) is never attempted.
wrangler() {
  echo "$*" >> "$wrangler_log"
  case "$*" in
    *b.parquet*) return 1 ;;
    *) return 0 ;;
  esac
}

check_not "vb_sh_r2_upload_all returns non-zero when an upload fails" \
  vb_sh_r2_upload_all "bucket/prefix" "$tmpdir/b.parquet" "$tmpdir/a.parquet"
if grep -q "a.parquet" "$wrangler_log"; then
  fail "fail-fast: a.parquet was attempted after b.parquet failed"
else
  pass "fail-fast: no further uploads attempted after the first failure"
fi

check "vb_sh_r2_upload_all succeeds when all uploads succeed" \
  vb_sh_r2_upload_all "bucket/prefix" "$tmpdir/a.parquet"

# 5b. vb_sh_r2_manifest_last refuses when upload_errors != 0.
out_r2_refused="$tmpdir/r2_refused.json"
check_not "vb_sh_r2_manifest_last returns non-zero when upload_errors=1" \
  vb_sh_r2_manifest_last "bucket/prefix" 1 "$out_r2_refused" "$tmpdir/a.parquet"
check_not "vb_sh_r2_manifest_last does NOT write a manifest when upload_errors=1" \
  test -f "$out_r2_refused"

# 5c. First publish (no previous manifest on R2): schema-valid, uploaded
#     under the canonical <prefix>/bus_manifest.json key with no-cache.
: > "$wrangler_log"
wrangler() {
  echo "$*" >> "$wrangler_log"
  case "$1 $2 $3" in
    "r2 object get") return 1 ;;   # no previous manifest
    "r2 object put") return 0 ;;
  esac
  return 1
}

out_r2_fresh="$tmpdir/r2_fresh.json"
check "vb_sh_r2_manifest_last succeeds on first publish" \
  vb_sh_r2_manifest_last "bucket/prefix" 0 "$out_r2_fresh" "$tmpdir/a.parquet"
check "R2 manifest file is written locally" test -f "$out_r2_fresh"
if [ -f "$out_r2_fresh" ]; then
  check "R2 manifest schema_version == 1" jq -e '.schema_version == 1' "$out_r2_fresh"
  check "R2 manifest tag == bucket-prefix" jq -e '.tag == "bucket/prefix"' "$out_r2_fresh"
  check "R2 manifest has exactly one asset entry" jq -e '(.assets | length) == 1' "$out_r2_fresh"
fi
if grep -q "r2 object put bucket/prefix/bus_manifest.json" "$wrangler_log"; then
  pass "R2 manifest uploaded under canonical <prefix>/bus_manifest.json key"
else
  fail "R2 manifest NOT uploaded under canonical key (log: $(cat "$wrangler_log"))"
fi
if grep "bus_manifest.json" "$wrangler_log" | grep -q -- "--cache-control no-cache"; then
  pass "R2 manifest uploaded with no-cache"
else
  fail "R2 manifest upload missing no-cache cache-control"
fi

# 5d. Carry-forward from an existing R2 manifest: a prefix-resident file not
#     in this call's list survives the merge.
wrangler() {
  case "$1 $2 $3" in
    "r2 object get")
      # args: r2 object get <key> --file <out> --remote
      cp "$prev_dir/bus_manifest.json" "$6"
      return 0 ;;
    "r2 object put") return 0 ;;
  esac
  return 1
}

out_r2_carry="$tmpdir/r2_carry.json"
check "vb_sh_r2_manifest_last succeeds with a previous R2 manifest" \
  vb_sh_r2_manifest_last "bucket/prefix" 0 "$out_r2_carry" "$tmpdir/a.parquet"
if [ -f "$out_r2_carry" ]; then
  check "R2 carry-forward: this run's file present" \
    jq -e '[.assets[].name] | index("a.parquet") != null' "$out_r2_carry"
  check "R2 carry-forward: previous-manifest-only file survives" \
    jq -e '[.assets[].name] | index("old_only.parquet") != null' "$out_r2_carry"
fi

# ---------------------------------------------------------------------------
# 6. vb_sh_safe_upload / vb_sh_restore: the old asset must survive every
#    failure before the swap (audit: vault/plans/CLOBBER-AUDIT-2026-10-08.md).
# ---------------------------------------------------------------------------
echo "new content" > "$tmpdir/a.parquet"

# 6a. Happy path: replaced in place, one asset, no temp leftovers.
fake_reset
fake_put a.parquet "old content"
check "safe upload succeeds over an existing asset" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_names)" = "a.parquet" ] && [ "$(fake_content a.parquet)" = "new content" ]; then
  pass "safe upload: one asset named a.parquet holding the new content"
else
  fail "safe upload: release holds '$(fake_names)', content '$(fake_content a.parquet)'"
fi

# 6b. Upload fails -> old asset untouched.
fake_reset
fake_put a.parquet "old content"
FAKE_UPLOAD_FAIL=1
check_not "safe upload returns non-zero when the upload fails" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_names)" = "a.parquet" ] && [ "$(fake_content a.parquet)" = "old content" ]; then
  pass "failed upload leaves the old asset in place"
else
  fail "failed upload changed the release: '$(fake_names)'"
fi

# 6c. Upload never completes (state stays "starter") -> old asset untouched.
fake_reset
fake_put a.parquet "old content"
FAKE_UPLOAD_STUCK=1
check_not "safe upload returns non-zero when the upload never completes" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_content a.parquet)" = "old content" ]; then
  pass "incomplete upload leaves the old asset in place"
else
  fail "incomplete upload deleted or changed the old asset"
fi

# 6d. Deleting the old asset fails -> old asset untouched, FAIL.
fake_reset
fake_put a.parquet "old content"
FAKE_DELETE_FAIL=1
check_not "safe upload returns non-zero when the old asset can't be deleted" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_content a.parquet)" = "old content" ]; then
  pass "failed delete leaves the old asset in place"
else
  fail "failed delete still lost the old asset"
fi

# 6e. Rename fails after the delete -> FAIL, and vb_sh_restore recovers the
#     new data from the temp copy.
fake_reset
fake_put a.parquet "old content"
FAKE_PATCH_FAIL=1
check_not "safe upload returns non-zero when the final rename fails" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
FAKE_PATCH_FAIL=0
restore_dir="$tmpdir/restore_6e"
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$restore_dir" >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 0 ] && [ "$(cat "$restore_dir/a.parquet" 2>/dev/null)" = "new content" ]; then
  pass "restore falls back to the unswapped temp copy"
else
  fail "restore after a failed rename: rc=$rc, content '$(cat "$restore_dir/a.parquet" 2>/dev/null)'"
fi

# 6f. The next successful upload clears the stranded temp copy.
check "safe upload succeeds after a stranded temp copy" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_names)" = "a.parquet" ]; then
  pass "stranded temp copy cleaned up by the next upload"
else
  fail "release still holds '$(fake_names)' after the next upload"
fi

# 6g. vb_sh_restore return codes: absent -> 2, download failure -> 1,
#     truncated download -> 1, never 0 on a bad file.
fake_reset
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 2 ] && pass "restore returns 2 for an absent asset" || fail "restore returned $rc for an absent asset, expected 2"
fake_put a.parquet "old content"
FAKE_DOWNLOAD_FAIL=1
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 when a listed asset can't be downloaded" || fail "restore returned $rc on a download failure, expected 1"
FAKE_DOWNLOAD_FAIL=0; FAKE_DOWNLOAD_TRUNC=1
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 on a truncated download" || fail "restore returned $rc on a truncated download, expected 1"
FAKE_DOWNLOAD_TRUNC=0; FAKE_LIST_FAIL=1
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 when the listing fails (never 'absent')" || fail "restore returned $rc on a listing failure, expected 1"
# An asset under its real name in a non-uploaded state (killed upload) is
# broken, not absent.
rc=0; VB_SH_ASSETS_JSON='[{"id":1,"name":"a.parquet","size":0,"state":"starter","created_at":"x"}]' \
  vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 for a half-uploaded asset (never 'absent')" || fail "restore returned $rc for a half-uploaded asset, expected 1"
# A listing jq can't parse (the 2026-10-08 dev dry run: jq died with
# "Argument list too long" and the old code read that as absent).
FAKE_LIST_FAIL=0
rc=0; VB_SH_ASSETS_JSON="not json" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 when jq fails on the listing (never 'absent')" || fail "restore returned $rc when jq failed, expected 1"

# 6g2. The listing is trimmed to the fields used, so it stays far below the
#      128 KB per-variable limit even for opta-latest's 134 assets.
#      This fake returns the raw release object (with an uploader per asset,
#      as GitHub does) and applies the caller's --jq, like gh api.
gh() {
  command jq -c '{assets: map(. + {uploader: {login: "x", id: 1}, label: ""})}' "$FAKE_DIR/assets.json" \
    | command jq -c "$4"
}
listing=$(vb_sh_list_assets "test/fixture" "test-tag")
if command jq -e 'length == 1 and all(.[]; (keys | sort) == ["created_at","id","name","size","state"])' <<<"$listing" >/dev/null; then
  pass "vb_sh_list_assets keeps only id/name/size/state/created_at"
else
  fail "vb_sh_list_assets returned extra or missing fields: $listing"
fi
gh() { gh_fake "$@"; }

# 6h. vb_sh_upload_all keeps the OK/FAIL line format panna's epv-pipeline.yml
#     greps ('^FAIL').
fake_reset
out=$(vb_sh_upload_all "test/fixture" "test-tag" "$tmpdir/a.parquet" 2>/dev/null)
grep -qx "OK a.parquet" <<<"$out" && pass "upload_all prints 'OK <name>'" || fail "upload_all printed '$out'"
FAKE_UPLOAD_FAIL=1
out=$(vb_sh_upload_all "test/fixture" "test-tag" "$tmpdir/a.parquet" 2>/dev/null)
grep -q "^FAIL a.parquet" <<<"$out" && pass "upload_all prints 'FAIL <name> ...'" || fail "upload_all printed '$out'"
# panna's epv-pipeline.yml runs `out=$(vb_sh_upload_all ...)` under
# `set -euo pipefail`; a failing LAST file must not kill the step before the
# FAIL line is read.
if out=$(set -e; vb_sh_upload_all "test/fixture" "test-tag" "$tmpdir/a.parquet" 2>/dev/null) \
   && grep -q "^FAIL a.parquet" <<<"$out"; then
  pass "upload_all returns 0 with a failing last file, so set -e callers still read FAIL"
else
  fail "upload_all aborted a set -e caller on a failing last file"
fi

# 6i. No workflow in this repo uploads with --clobber any more.
clobber_uploads=$(grep -n 'release upload.*--clobber' "$SCRIPT_DIR"/../../.github/workflows/*.yml "$SCRIPT_DIR/../versebus.sh" 2>/dev/null \
  | grep -v ':[0-9]*:[[:space:]]*#' || true)
if [ -z "$clobber_uploads" ]; then
  pass "no 'gh release upload --clobber' left in workflows or versebus.sh"
else
  fail "delete-first uploads remain: $clobber_uploads"
fi

echo ""
echo "TOTALS: $pass_count passed, $fail_count failed"
if [ "$fail_count" -gt 0 ]; then
  exit 1
fi
