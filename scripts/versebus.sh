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
VERSEBUS_SH_VERSION="1.2.0"

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
# out_manifest_path is only where the manifest JSON is written LOCALLY; the
# release asset is ALWAYS uploaded as bus_manifest.json (gh release upload
# names assets by file basename, so a caller passing e.g.
# models_manifest.json would otherwise publish under the wrong name and no
# consumer would ever find the manifest — this exact miss shipped and was
# caught live 2026-07-17).
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

  local upload_src="$out_path"
  if [ "$(basename "$out_path")" != "bus_manifest.json" ]; then
    cp "$out_path" "$tmpdir/bus_manifest.json" || { rm -rf "$tmpdir"; return 1; }
    upload_src="$tmpdir/bus_manifest.json"
  fi

  if gh release upload "$tag" "$upload_src" --repo "$repo" --clobber; then
    echo "OK bus_manifest.json (generation $generation, $(echo "$entries" | jq 'length') asset(s))"
    rm -rf "$tmpdir"
    return 0
  else
    echo "::error::Failed to upload bus_manifest.json for ${repo}@${tag}" >&2
    rm -rf "$tmpdir"
    return 1
  fi
}

# --- R2 mirror of the trio (FABLE-VERSEBUS-PHASE5-PLAN P5-A) ----------------
# Same §1.2 manifest contract as the release functions, for workflows that
# publish to Cloudflare R2 via wrangler instead of GitHub Releases. R2 has no
# multi-object transactions; these buy (a) the torn window is never
# advertised (no manifest for a torn generation), (b) the run is red at the
# moment of tearing, (c) a re-run heals (puts are idempotent per key). Full
# atomicity needs reader-side pointer adoption — out of scope per PD4.

# vb_sh_r2_upload_all <bucket-prefix> <file> [<file> ...]
# Uploads each file to <bucket-prefix>/<basename>. ONE deliberate difference
# from vb_sh_upload_all: FAIL-FAST — prints "FAIL <name>" and returns 1 on
# the first error, leaving remaining files un-uploaded (ratified P5-A choice:
# smallest torn window, and the manifest below is never written for the torn
# set). Prints "OK <name>" per success. Cache-Control lets browsers/CF cache
# the object and revalidate via ETag (304, zero bytes) instead of
# re-downloading megabytes — the blog drops its cache-busting query once
# objects carry this (blog #388).
vb_sh_r2_upload_all() {
  local prefix="$1"; shift
  local f name
  for f in "$@"; do
    [ -f "$f" ] || continue
    name=$(basename "$f")
    if wrangler r2 object put "${prefix}/${name}" --file "$f" \
         --cache-control "public, max-age=300" --remote; then
      echo "OK $name"
    else
      echo "FAIL $name"
      return 1
    fi
  done
}

# vb_sh_r2_manifest_last <bucket-prefix> <upload_errors> <out_manifest_path> <file> [<file> ...]
# R2 twin of vb_sh_manifest_last: refuses when upload_errors != 0; builds the
# §1.2 manifest (sha256/bytes/generation/producer; tag = the bucket-prefix);
# CARRIES FORWARD entries from the existing <bucket-prefix>/bus_manifest.json
# whose basename isn't in this call's file list (a partial publish — e.g.
# sync-game-logs-r2.yml's game-logs* subset — still describes the whole
# prefix). Uploads LAST, always under the canonical key
# <bucket-prefix>/bus_manifest.json, with no-cache so readers never act on a
# stale commit record. Returns non-zero (manifest NOT uploaded) on any
# internal failure.
vb_sh_r2_manifest_last() {
  local prefix="$1" upload_errors="$2" out_path="$3"; shift 3

  if [ "$upload_errors" -ne 0 ]; then
    echo "::error::vb_sh_r2_manifest_last refusing to update bus_manifest.json for ${prefix} -- ${upload_errors} upload failure(s) this run" >&2
    return 1
  fi

  local tmpdir prev_manifest
  tmpdir=$(mktemp -d) || return 1
  prev_manifest=""
  if wrangler r2 object get "${prefix}/bus_manifest.json" \
       --file "$tmpdir/prev_bus_manifest.json" --remote 2>/dev/null \
     && [ -s "$tmpdir/prev_bus_manifest.json" ]; then
    prev_manifest="$tmpdir/prev_bus_manifest.json"
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

  if [ -n "$prev_manifest" ]; then
    entries=$(jq -n --argjson new "$entries" --slurpfile prev "$prev_manifest" '
      ($new | map(.name)) as $new_names
      | $new + [$prev[0].assets[]? | select(.name as $n | ($new_names | index($n)) == null)]
    ') || { rm -rf "$tmpdir"; return 1; }
  fi

  local generation produced_at
  generation="$(date -u +%Y%m%dT%H%M%SZ)-r${GITHUB_RUN_ID:-local}"
  produced_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  jq -n --arg tag "$prefix" --arg gen "$generation" --arg produced "$produced_at" \
    --arg repo "${GITHUB_REPOSITORY:-local}" --arg workflow "${GITHUB_WORKFLOW:-local}" \
    --arg run_id "${GITHUB_RUN_ID:-}" --arg run_attempt "${GITHUB_RUN_ATTEMPT:-}" \
    --argjson assets "$entries" \
    '{schema_version: 1, tag: $tag, generation: $gen, produced_at_utc: $produced,
      producer: {repo: $repo, workflow: $workflow, run_id: $run_id, run_attempt: $run_attempt},
      assets: $assets, notes: ""}' > "$out_path" || { rm -rf "$tmpdir"; return 1; }

  if wrangler r2 object put "${prefix}/bus_manifest.json" --file "$out_path" \
       --cache-control "no-cache" --remote; then
    echo "OK bus_manifest.json (generation $generation, $(echo "$entries" | jq 'length') asset(s))"
    rm -rf "$tmpdir"
    return 0
  else
    echo "::error::Failed to upload bus_manifest.json for ${prefix}" >&2
    rm -rf "$tmpdir"
    return 1
  fi
}
