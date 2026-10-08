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
VERSEBUS_SH_VERSION="1.3.0"

# Never `gh release upload --clobber`: clobber DELETES the existing asset
# before it uploads, so an upload that then fails (wheather, 2026-09-17: HTTP
# 500 on a 115 MB asset) leaves the release without the file. Every
# accumulating writer here reads its asset back next run, and a missing one
# reads as "first run" -- the next run publishes a cut-down file over the
# history. vb_sh_safe_upload keeps the old asset until a verified
# replacement is on the release. Audit: vault/plans/CLOBBER-AUDIT-2026-10-08.md.
VB_SH_TMP_PREFIX="vbnew-"

# vb_sh_list_assets <repo> <tag>
# Prints the release's asset array as JSON, trimmed to the fields used here.
# Non-zero if it can't be fetched. Trimmed because the full listing carries
# an uploader object per asset: opta-latest's 134 assets came to more than
# Linux's 128 KB limit for one environment string, and a caller that
# exported it broke every later exec ("Argument list too long", 2026-10-08).
# Keep VB_SH_ASSETS_JSON a plain shell variable; never export it.
vb_sh_list_assets() {
  gh api "repos/$1/releases/tags/$2" --jq '[.assets[] | {id, name, size, state, created_at}]'
}

# vb_sh_safe_upload <repo> <tag> <file>
# Uploads <file> under a temporary asset name (vbnew-<run>-<attempt>-<pid>--
# <name>), confirms it reached state "uploaded" at the local byte size, and
# only then deletes the old <name> (plus any temp copies left by earlier
# runs) and renames the new asset to <name>. Prints "OK <name>" or
# "FAIL <name> <why>". Failure before the delete leaves the old asset
# untouched; failure of the final rename leaves the data on the release
# under its temp name, where vb_sh_restore finds it.
vb_sh_safe_upload() {
  local repo="$1" tag="$2" f="$3"
  local name size tmpname tmpd assets new_id attempt id old_ids
  name=$(basename "$f")
  [ -f "$f" ] || { echo "FAIL $name local file missing"; return 1; }
  size=$(stat -c%s "$f")
  # Unique per call: a second upload of the same file in one shell must not
  # collide with a temp copy the first one stranded.
  tmpname="${VB_SH_TMP_PREFIX}${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$${RANDOM}${RANDOM}--${name}"

  # gh names an asset after the file's basename, so stage it under the temp
  # name. Hard link where possible: the events files run to ~300 MB.
  tmpd=$(mktemp -d) || { echo "FAIL $name could not create a staging dir"; return 1; }
  ln "$f" "$tmpd/$tmpname" 2>/dev/null || cp "$f" "$tmpd/$tmpname" || {
    rm -rf "$tmpd"; echo "FAIL $name could not stage $tmpname"; return 1; }
  if ! gh release upload "$tag" "$tmpd/$tmpname" --repo "$repo"; then
    rm -rf "$tmpd"
    echo "FAIL $name upload failed (old asset untouched)"
    return 1
  fi
  rm -rf "$tmpd"

  # The listing can lag an upload by seconds (versebus.R saw a stale size
  # for ~95s on 2026-07-16), so poll before declaring the upload bad.
  new_id=""
  for attempt in 1 2 3 4 5; do
    if assets=$(vb_sh_list_assets "$repo" "$tag"); then
      # A jq failure leaves new_id empty, which only ever means "keep the old
      # asset" -- never a delete.
      new_id=$(jq -r --arg n "$tmpname" --argjson s "$size" \
        'first(.[] | select(.name == $n and .state == "uploaded" and .size == $s) | .id) // empty' <<<"$assets") || new_id=""
      [ -n "$new_id" ] && break
    fi
    [ "$attempt" -lt 5 ] && sleep $((attempt * 3))
  done
  if [ -z "$new_id" ]; then
    echo "FAIL $name $tmpname never listed as uploaded at $size bytes (old asset untouched)"
    return 1
  fi

  # Delete the old asset and any stale temp copies of it. A failed delete of
  # the real <name> stops here: the rename below would collide with it.
  if ! old_ids=$(jq -r --arg n "$name" --arg t "$tmpname" --arg p "$VB_SH_TMP_PREFIX" \
    '.[] | select(.name == $n or (.name != $t and (.name | startswith($p)) and (.name | endswith("--" + $n)))) | "\(.id) \(.name)"' <<<"$assets"); then
    echo "FAIL $name could not read the listing to find the old asset; new copy left as $tmpname"
    return 1
  fi
  while read -r id old_name; do
    [ -n "$id" ] || continue
    if ! gh api -X DELETE "repos/${repo}/releases/assets/${id}" >/dev/null; then
      if [ "$old_name" = "$name" ]; then
        echo "FAIL $name could not delete the old asset; new copy left as $tmpname"
        return 1
      fi
      echo "::warning::could not delete stale temp asset $old_name" >&2
    fi
  done <<<"$old_ids"

  for attempt in 1 2 3; do
    if gh api -X PATCH "repos/${repo}/releases/assets/${new_id}" -f name="$name" >/dev/null; then
      echo "OK $name"
      return 0
    fi
    [ "$attempt" -lt 3 ] && sleep $((attempt * 5))
  done
  echo "FAIL $name rename failed; the data is on the release as $tmpname (vb_sh_restore falls back to it)"
  return 1
}

# vb_sh_restore <repo> <tag> <name> <dir>
# Downloads <name> into <dir>. If <name> is absent but an unswapped temp
# copy from vb_sh_safe_upload is on the release, downloads the newest one
# and saves it as <dir>/<name>. Checks the downloaded size against the
# listing. Uses $VB_SH_ASSETS_JSON as the listing when set (callers that
# restore many files list once). Returns:
#   0  restored
#   2  not on the release at all (neither the name nor a temp copy)
#   1  on the release but could not be downloaded intact -- callers must
#      treat this as fatal, never as "absent"
vb_sh_restore() {
  local repo="$1" tag="$2" name="$3" dir="$4"
  local assets src want attempt got
  assets="${VB_SH_ASSETS_JSON:-}"
  if [ -z "$assets" ]; then
    assets=$(vb_sh_list_assets "$repo" "$tag") || return 1
  fi
  # Every jq failure here returns 1: an empty answer from a broken jq must
  # never read as "absent" (it did once, see vb_sh_list_assets).
  src=$(jq -r --arg n "$name" \
    '[.[] | select(.state == "uploaded" and .name == $n)] | .[0].name // empty' <<<"$assets") || return 1
  if [ -z "$src" ]; then
    # Listed under its real name but not "uploaded" (a killed upload) is a
    # broken asset, not an absent one.
    local half
    half=$(jq -r --arg n "$name" '[.[] | select(.name == $n)] | length' <<<"$assets") || return 1
    if [ "$half" != "0" ]; then
      echo "::error::$name is on ${repo}@${tag} but not in state uploaded" >&2
      return 1
    fi
    src=$(jq -r --arg n "$name" --arg p "$VB_SH_TMP_PREFIX" \
      '[.[] | select(.state == "uploaded" and (.name | startswith($p)) and (.name | endswith("--" + $n)))]
       | sort_by(.created_at) | last | .name // empty' <<<"$assets") || return 1
    [ -n "$src" ] && echo "::warning::$name is missing from ${repo}@${tag}; restoring the unswapped upload $src" >&2
  fi
  [ -n "$src" ] || return 2
  want=$(jq -r --arg n "$src" '[.[] | select(.name == $n)] | .[0].size' <<<"$assets") || return 1

  mkdir -p "$dir" || return 1
  for attempt in 1 2 3; do
    if gh release download "$tag" --repo "$repo" --pattern "$src" --dir "$dir" --clobber; then
      got=$(stat -c%s "$dir/$src" 2>/dev/null || echo -1)
      if [ "$got" = "$want" ]; then
        [ "$src" = "$name" ] || mv -f "$dir/$src" "$dir/$name" || return 1
        return 0
      fi
      echo "::warning::$src downloaded at $got bytes, listing says $want (attempt $attempt)" >&2
    fi
    [ "$attempt" -lt 3 ] && sleep $((attempt * 5))
  done
  return 1
}

# vb_sh_upload_all <repo> <tag> <file> [<file> ...]
# vb_sh_safe_upload on each file, one at a time. Prints "OK <name>" or
# "FAIL <name> ..." per file to stdout -- the caller greps/counts failures
# (panna's epv-pipeline.yml greps '^FAIL'). Never aborts on an individual
# failure and always returns 0: callers run `out=$(vb_sh_upload_all ...)`
# under `set -e`, where a non-zero return would kill the step before the
# FAIL lines are read. The caller decides whether to gate downstream steps
# (verify, manifest) on the failure count.
vb_sh_upload_all() {
  local repo="$1" tag="$2"; shift 2
  local f
  for f in "$@"; do
    [ -f "$f" ] || continue
    vb_sh_safe_upload "$repo" "$tag" "$f" || true
  done
  return 0
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
# vb_sh_safe_upload. Returns non-zero (manifest NOT uploaded) on any internal
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
  # A previous manifest that exists but fails to download must not read as
  # "first publish": the merged manifest would silently drop every
  # carried-forward entry.
  local rc=0
  VB_SH_ASSETS_JSON="" vb_sh_restore "$repo" "$tag" "bus_manifest.json" "$tmpdir" || rc=$?
  if [ "$rc" -eq 0 ]; then
    prev_manifest="$tmpdir/bus_manifest.json"
  elif [ "$rc" -ne 2 ]; then
    echo "::error::vb_sh_manifest_last could not read the previous bus_manifest.json for ${repo}@${tag}" >&2
    rm -rf "$tmpdir"
    return 1
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

  if vb_sh_safe_upload "$repo" "$tag" "$upload_src" >/dev/null; then
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
