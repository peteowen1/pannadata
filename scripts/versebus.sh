#!/usr/bin/env bash
# versebus.sh -- bash mirror of the versebus data-bus hardening pattern
# (ECOSYSTEM-FIX-PLAN.md Section 1.4) for workflows that upload release
# assets via raw `gh` CLI in bash instead of R's vb_publish() (R/versebus.R,
# vendored in panna/bouncer/torp/*models). pannadata's daily-opta-scrape.yml
# is the only writer that publishes this way -- torpdata uploads via torp,
# bouncerdata via bouncer (both R).
#
# Source this file (`source scripts/versebus.sh`) from a workflow step. Every
# function takes an explicit repo/tag -- no repo-specific glue lives here.
# Canonical manifest schema: ECOSYSTEM-FIX-PLAN.md Section 1.2
# (bus_manifest.json, schema_version 1) -- identical shape to the JSON
# R/versebus.R's vb_write_manifest() produces, so a strict R consumer reading
# a tag this script published sees the same contract.
VERSEBUS_SH_VERSION="1.0.0"

# vb_sh_upload_all <repo> <tag> <file> [<file> ...]
# Uploads each file with `gh release upload --clobber`, one at a time.
# Prints "OK <name>" or "FAIL <name>" per file to stdout -- the caller greps/
# counts failures (mirrors the pre-existing `upload_errors` counter
# convention in daily-opta-scrape.yml). Never aborts on an individual
# failure; the caller decides whether to gate downstream steps (verify,
# manifest) on the failure count.
vb_sh_upload_all() {
  local repo="$1" tag="$2"; shift 2
  local f
  for f in "$@"; do
    [ -f "$f" ] || continue
    if gh release upload "$tag" "$f" --repo "$repo" --clobber; then
      echo "OK $(basename "$f")"
    else
      echo "FAIL $(basename "$f")"
    fi
  done
}

# vb_sh_verify <repo> <tag> <file> [<file> ...]
# Re-fetches the LIVE asset list (uncached) and compares byte size for each
# file. Prints "OK <name>" / "FAIL <name> size <live> != local <local>" /
# "FAIL <name> missing from live asset list". Mirrors vb_publish()'s
# post-upload verify step -- catches a --clobber that silently landed
# truncated (network blip mid-upload).
vb_sh_verify() {
  local repo="$1" tag="$2"; shift 2
  local live_json
  live_json=$(gh api "repos/${repo}/releases/tags/${tag}" --jq '.assets' 2>/dev/null) || {
    echo "FAIL <listing> could not fetch live asset list for ${repo}@${tag}"
    return 1
  }
  local f name local_size live_size failed=0
  for f in "$@"; do
    [ -f "$f" ] || continue
    name=$(basename "$f")
    local_size=$(stat -c%s "$f")
    live_size=$(echo "$live_json" | jq -r --arg n "$name" '.[] | select(.name==$n) | .size' | head -1)
    if [ -z "$live_size" ]; then
      echo "FAIL $name missing from live asset list"
      failed=1
    elif [ "$live_size" != "$local_size" ]; then
      echo "FAIL $name size $live_size != local $local_size"
      failed=1
    else
      echo "OK $name"
    fi
  done
  return $failed
}

# vb_sh_manifest_last <repo> <tag> <upload_errors> <out_manifest_path> <file> [<file> ...]
# Refuses (non-zero exit, no manifest write/upload) when upload_errors != 0
# -- the manifest-last gate: the previous manifest remains the commit
# record so consumers keep seeing the last consistent snapshot. Builds
# bus_manifest.json (sha256sum + jq per file) and CARRIES FORWARD any
# previous manifest entries whose basename isn't in this call's file list --
# a partial-tag publish (e.g. a run that only re-uploaded a few per-league
# events_<comp>.parquet files because the concurrent-scrape mtime-skip logic
# left the rest untouched) still describes the WHOLE tag, per
# ECOSYSTEM-FIX-PLAN.md Section 1.2. Uploads the manifest LAST via
# --clobber. Returns non-zero (manifest NOT uploaded) on any internal
# failure (fetch/hash/jq), same effect as the upload_errors gate.
vb_sh_manifest_last() {
  local repo="$1" tag="$2" upload_errors="$3" out_path="$4"; shift 4

  if [ "$upload_errors" -ne 0 ]; then
    echo "::error::vb_sh_manifest_last refusing to update bus_manifest.json for ${repo}@${tag} -- ${upload_errors} upload failure(s) this run" >&2
    return 1
  fi

  local tmpdir prev_manifest
  tmpdir=$(mktemp -d) || return 1
  prev_manifest=""
  if gh release download "$tag" --repo "$repo" --pattern "bus_manifest.json" --dir "$tmpdir" 2>/dev/null; then
    prev_manifest="$tmpdir/bus_manifest.json"
  fi

  local entries="[]" f name sha bytes entry
  for f in "$@"; do
    [ -f "$f" ] || continue
    name=$(basename "$f")
    sha=$(sha256sum "$f" | cut -d' ' -f1) || { rm -rf "$tmpdir"; return 1; }
    bytes=$(stat -c%s "$f") || { rm -rf "$tmpdir"; return 1; }
    entry=$(jq -n --arg name "$name" --arg sha "$sha" --argjson bytes "$bytes" \
      '{name: $name, sha256: $sha, bytes: $bytes, rows: null}') || { rm -rf "$tmpdir"; return 1; }
    entries=$(echo "$entries" | jq --argjson e "$entry" '. + [$e]') || { rm -rf "$tmpdir"; return 1; }
  done

  if [ -n "$prev_manifest" ] && [ -f "$prev_manifest" ]; then
    entries=$(jq -n --argjson new "$entries" --slurpfile prev "$prev_manifest" '
      ($new | map(.name)) as $new_names
      | $new + [$prev[0].assets[]? | select(.name as $n | ($new_names | index($n)) == null)]
    ') || { rm -rf "$tmpdir"; return 1; }
  fi

  local generation produced_at
  generation="$(date -u +%Y%m%dT%H%M%SZ)-r${GITHUB_RUN_ID:-local}"
  produced_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  jq -n --arg tag "$tag" --arg gen "$generation" --arg produced "$produced_at" \
    --arg repo "${GITHUB_REPOSITORY:-local}" --arg workflow "${GITHUB_WORKFLOW:-local}" \
    --arg run_id "${GITHUB_RUN_ID:-}" --arg run_attempt "${GITHUB_RUN_ATTEMPT:-}" \
    --argjson assets "$entries" \
    '{schema_version: 1, tag: $tag, generation: $gen, produced_at_utc: $produced,
      producer: {repo: $repo, workflow: $workflow, run_id: $run_id, run_attempt: $run_attempt},
      assets: $assets, notes: ""}' > "$out_path" || { rm -rf "$tmpdir"; return 1; }

  if gh release upload "$tag" "$out_path" --repo "$repo" --clobber; then
    echo "OK bus_manifest.json (generation $generation, $(echo "$entries" | jq 'length') asset(s))"
    rm -rf "$tmpdir"
    return 0
  else
    echo "::error::Failed to upload bus_manifest.json for ${repo}@${tag}" >&2
    rm -rf "$tmpdir"
    return 1
  fi
}
