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
gh() {
  if [ "$1" = "release" ] && [ "$2" = "download" ]; then
    return 1  # no previous manifest -- simulates a first-ever publish
  elif [ "$1" = "release" ] && [ "$2" = "upload" ]; then
    return 0
  fi
  echo "unexpected gh invocation in phase 2: $*" >&2
  return 1
}

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

gh() {
  if [ "$1" = "release" ] && [ "$2" = "download" ]; then
    local dir_arg="" prevarg=""
    for a in "$@"; do
      if [ "$prevarg" = "--dir" ]; then dir_arg="$a"; fi
      prevarg="$a"
    done
    if [ -n "$dir_arg" ]; then
      cp "$prev_dir/bus_manifest.json" "$dir_arg/bus_manifest.json"
      return 0
    fi
    return 1
  elif [ "$1" = "release" ] && [ "$2" = "upload" ]; then
    return 0
  fi
  echo "unexpected gh invocation in phase 3: $*" >&2
  return 1
}

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
# 4. YAML ordering regression guard (grep-based, no yaml lib dependency):
#    daily-opta-scrape.yml must upload opta-manifest.parquet AFTER the
#    opta_*.parquet loop, and bus_manifest.json's own call must come after
#    that -- guards against a future edit silently regressing panna C1
#    (manifest-first, ungated).
# ---------------------------------------------------------------------------
workflow="$SCRIPT_DIR/../../.github/workflows/daily-opta-scrape.yml"
if [ -f "$workflow" ]; then
  parquet_loop_line=$(grep -n 'for f in opta/opta_\*\.parquet' "$workflow" | head -1 | cut -d: -f1)
  domain_manifest_line=$(grep -n 'gh release upload opta-latest opta-manifest\.parquet' "$workflow" | head -1 | cut -d: -f1)
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

echo ""
echo "TOTALS: $pass_count passed, $fail_count failed"
if [ "$fail_count" -gt 0 ]; then
  exit 1
fi
