// Backfill football player headshots to R2 from Wikimedia Commons (CC-licensed),
// + sharpen nationality via Wikidata P1532 "country for sport" (FIFA nation).
//
// Pipeline (keyed by Opta player_id, via football/player-bio.json's `wd` QID):
//   1. Wikidata wbgetentities (≤50/req): P18 (image filename) + P1532 (FIFA nation QID)
//   2. Resolve the distinct P1532 country QIDs → English label → flag ISO
//   3. Commons imageinfo (≤40/req): 320px thumb URL + license + author + file page;
//      keep only FREE licenses (CC0 / PD / CC-BY / CC-BY-SA), skip anything else
//   4. Fetch the 320px thumb → re-encode webp → R2 football/headshots/{player_id}.webp
//   5. Write + upload football/headshot-credits.json (player_id → author/license/source)
//      and an updated football/player-bio.json (nationality from P1532 where present)
//
// CC-BY(-SA) REQUIRES attribution → the blog shows a per-headshot tooltip and a
// /photo-credits page from headshot-credits.json. CC0/PD need none but are credited too.
//
// Deps (outside repo): npm i hyparquet csv-parse i18n-iso-countries sharp
// Needs: football/player-bio.json on R2 (from build-football-bio.mjs); wrangler (R2 write).
// Run: node build-football-headshots.mjs            (full backfill)
//      node build-football-headshots.mjs --limit=40 (first N players, for testing)
//      node build-football-headshots.mjs --dry       (metadata only, no fetch/upload)
// TODO: fold into pannadata build-blog-data.yml to refresh with the data.

import sharp from 'sharp'
import countries from 'i18n-iso-countries'
import en from 'i18n-iso-countries/langs/en.json' with { type: 'json' }
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { writeFile, mkdir, rm } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
countries.registerLocale(en)
const exec = promisify(execFile)

const R2_PUB = "https://pub-ee4bf5b599a047f9ac2b9facc1587008.r2.dev/football/"
const UA = { "User-Agent": "inthegame-blog/1.0 (https://inthegame.blog; fptpost@gmail.com)" }
// wrangler binary: CI (linux) has it on PATH as `wrangler`; local Windows uses the
// npm .cmd shim. Override with WRANGLER_BIN if needed.
const WRANGLER = process.env.WRANGLER_BIN ||
  (process.platform === "win32" ? path.join(os.homedir(), "AppData", "Roaming", "npm", "wrangler.cmd") : "wrangler")
const TMP = path.join(os.tmpdir(), "fbhead-img")
const DRY = process.argv.includes("--dry")
const REPROCESS = process.argv.includes("--reprocess")   // re-fetch+re-crop even if already on R2
const LIMIT = (process.argv.find(a => a.startsWith("--limit=")) || "").split("=")[1]
await mkdir(TMP, { recursive: true })

// Wikidata "country for sport" QID label → [flag ISO, display]. Football nations
// (home nations get their own flag) + overrides for labels i18n-iso-countries misses.
const NAT_OVR = {
  "England":["gb-eng","England"], "Scotland":["gb-sct","Scotland"], "Wales":["gb-wls","Wales"],
  "Northern Ireland":["gb-nir","Northern Ireland"], "Ivory Coast":["ci","Ivory Coast"],
  "Democratic Republic of the Congo":["cd","DR Congo"], "Republic of the Congo":["cg","Congo"],
  "Cape Verde":["cv","Cape Verde"], "South Korea":["kr","South Korea"], "North Macedonia":["mk","North Macedonia"],
  "The Gambia":["gm","Gambia"], "Bosnia and Herzegovina":["ba","Bosnia & Herzegovina"], "Kosovo":["xk","Kosovo"],
  "Czech Republic":["cz","Czechia"], "Moldova":["md","Moldova"], "Syria":["sy","Syria"], "Curaçao":["cw","Curaçao"],
}
const natToIso = (label) => {
  if (!label) return [null, null]
  if (NAT_OVR[label]) return NAT_OVR[label]
  const c = countries.getAlpha2Code(label, "en")
  return c ? [c.toLowerCase(), label] : [null, label]
}
const FREE = /^(cc0|cc[\- ]by|cc[\- ]by[\- ]sa|public domain|pd|pdm)/i

const chunk = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o }
const wapi = async (host, params) => {
  const u = `https://${host}/w/api.php?${new URLSearchParams({ format: "json", origin: "*", ...params })}`
  const r = await fetch(u, { headers: UA }); return r.json()
}

// ── Load our players (player_id → valid Wikidata QID) ──────────────
const bio = await (await fetch(R2_PUB + "player-bio.json?t=" + Math.floor(Math.random()*1e9))).json()
let players = Object.entries(bio).filter(([, b]) => /^Q\d+$/.test(b.wd || "")).map(([id, b]) => ({ id, qid: b.wd, bio: b }))
if (LIMIT) players = players.slice(0, +LIMIT)
console.log(`players with a Wikidata QID: ${players.length}`)

// ── 1) Wikidata: P18 (image) + P1532 (country for sport) ───────────
const byQid = new Map(players.map(p => [p.qid, p]))
const countryQids = new Set()
for (const batch of chunk([...byQid.keys()], 50)) {
  const j = await wapi("www.wikidata.org", { action: "wbgetentities", ids: batch.join("|"), props: "claims" })
  for (const qid of batch) {
    const c = j.entities?.[qid]?.claims || {}
    const p = byQid.get(qid)
    p.img = c.P18?.[0]?.mainsnak?.datavalue?.value || null
    p.cfs = c.P1532?.[0]?.mainsnak?.datavalue?.value?.id || null
    if (p.cfs) countryQids.add(p.cfs)
  }
}
const withImg = players.filter(p => p.img).length, withCfs = players.filter(p => p.cfs).length
console.log(`P18 image: ${withImg} (${(100*withImg/players.length).toFixed(0)}%) | P1532 nation: ${withCfs} (${(100*withCfs/players.length).toFixed(0)}%)`)

// ── 2) Resolve country-for-sport QIDs → label → ISO ────────────────
const cfsIso = new Map()
for (const batch of chunk([...countryQids], 50)) {
  const j = await wapi("www.wikidata.org", { action: "wbgetentities", ids: batch.join("|"), props: "labels", languages: "en" })
  for (const qid of batch) {
    const label = j.entities?.[qid]?.labels?.en?.value
    cfsIso.set(qid, natToIso(label))
  }
}

// ── 3) Commons imageinfo for P18 files: thumb + license + author ───
const imgPlayers = players.filter(p => p.img)
const fileToPlayers = new Map()
for (const p of imgPlayers) { const k = "File:" + p.img; if (!fileToPlayers.has(k)) fileToPlayers.set(k, []); fileToPlayers.get(k).push(p) }
let free = 0, nonfree = 0
for (const batch of chunk([...fileToPlayers.keys()], 40)) {
  const j = await wapi("commons.wikimedia.org", {
    action: "query", prop: "imageinfo", iiprop: "url|extmetadata", iiurlwidth: "320", titles: batch.join("|")
  })
  for (const pg of Object.values(j.query?.pages || {})) {
    const ii = pg.imageinfo?.[0]; if (!ii) continue
    const em = ii.extmetadata || {}
    const licShort = (em.LicenseShortName?.value || "").trim()
    const licCode = (em.License?.value || licShort).trim()
    const isFree = FREE.test(licCode) || FREE.test(licShort)
    const targets = fileToPlayers.get("File:" + decodeURIComponent(pg.title.replace(/^File:/, ""))) || fileToPlayers.get(pg.title) || []
    for (const p of targets) {
      if (!isFree) { p.img = null; nonfree++; continue }
      free++
      p.orig = ii.url       // original file — always served (no on-demand thumb generation → no 400s)
      p.credit = {
        a: (em.Artist?.value || "").replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim().slice(0, 80) || "Unknown",
        l: licShort || licCode,
        u: ii.descriptionurl || ("https://commons.wikimedia.org/wiki/" + encodeURIComponent(pg.title))
      }
    }
  }
}
console.log(`Commons: free ${free}, skipped non-free ${nonfree}`)

// ── 4/5) Build outputs: update bio nationality + credits; fetch+upload imgs ──
const credits = {}
const bioOut = { ...bio }
let validNumId = /^[A-Za-z0-9]+$/
let ok = 0, miss = 0, err = 0, done = 0
const toFetch = []
for (const p of players) {
  // nationality from P1532 (FIFA nation) where available, else keep reep value
  if (p.cfs && cfsIso.has(p.cfs)) {
    const [iso, nat] = cfsIso.get(p.cfs)
    if (nat) bioOut[p.id] = { ...bioOut[p.id], nat, ...(iso ? { iso } : {}) }
  }
  // credit is recorded only AFTER a successful upload (below), so credits.json
  // always matches what's actually on R2.
  if (p.orig && p.credit && validNumId.test(p.id)) toFetch.push(p)
}
console.log(`headshots to fetch: ${toFetch.length}`)

if (!DRY) {
  const sleep = ms => new Promise(r => setTimeout(r, ms))
  // Global rate gate: space Commons requests ≥500ms apart (≤~2/s) so a one-time
  // backfill never hammers a donation-funded service. R2 uploads (our bucket)
  // overlap freely. The run is resumable — photos already on R2 are skipped.
  const gate = { next: 0 }
  let skipped = 0
  async function one(p) {
    try {
      // Already on R2 from an earlier run? Keep its credit, don't re-fetch.
      // (--reprocess forces a re-fetch + re-crop of everything, e.g. after a
      // crop-strategy change.)
      if (!REPROCESS) {
        const head = await fetch(`${R2_PUB}headshots/${p.id}.webp`, { method: "HEAD" }).catch(() => null)
        if (head && head.ok) { credits[p.id] = p.credit; skipped++; ok++; return }
      }

      // Gentle, rate-gated fetch of the ORIGINAL (always served), with 429 respect.
      let res = null
      for (let a = 0; a < 5; a++) {
        const wait = gate.next - Date.now(); if (wait > 0) await sleep(wait)
        gate.next = Math.max(Date.now(), gate.next) + 500
        res = await fetch(p.orig, { headers: UA }).catch(() => null)
        if (res && res.ok) break
        if (res && res.status === 429) { const ra = +(res.headers.get("retry-after") || 5); await sleep(Math.min(ra, 30) * 1000) }
        else await sleep(800 * (a + 1))
      }
      if (!res || !res.ok) { miss++; return }
      if (+(res.headers.get("content-length") || 0) > 30 * 1024 * 1024) { miss++; return }
      // Content-aware crop: bias toward the most salient region (usually the
      // player/face) rather than blindly "top" — Commons photos are action shots,
      // so the subject isn't always at the top.
      const webp = await sharp(Buffer.from(await res.arrayBuffer())).resize(320, 320, { fit: "cover", position: sharp.strategy.attention }).webp({ quality: 82 }).toBuffer()
      const fp = path.join(TMP, `${p.id}.webp`)
      await writeFile(fp, webp)
      await exec(WRANGLER, ["r2", "object", "put", `inthegame-data/football/headshots/${p.id}.webp`, "--file", fp, "--remote", "--content-type", "image/webp"], { shell: true, maxBuffer: 1 << 24 })
      await rm(fp).catch(() => {})
      credits[p.id] = p.credit
      ok++
    } catch (e) { err++; if (err <= 4) console.error(`err ${p.id}: ${String(e.message).slice(0, 120)}`) }
    finally { if (++done % 100 === 0) console.log(`  ${done}/${toFetch.length} ok=${ok}(skip ${skipped}) miss=${miss} err=${err}`) }
  }
  const CONC = 3, q = toFetch.slice()   // 3 workers, but Commons fetches are gated to ~2/s globally
  await Promise.all(Array.from({ length: CONC }, async () => { while (q.length) await one(q.shift()) }))

  // upload credits + updated bio
  const cf = path.join(TMP, "headshot-credits.json"); await writeFile(cf, JSON.stringify(credits))
  const bf = path.join(TMP, "player-bio.json"); await writeFile(bf, JSON.stringify(bioOut))
  await exec(WRANGLER, ["r2", "object", "put", "inthegame-data/football/headshot-credits.json", "--file", cf, "--remote", "--content-type", "application/json"], { shell: true, maxBuffer: 1 << 24 })
  await exec(WRANGLER, ["r2", "object", "put", "inthegame-data/football/player-bio.json", "--file", bf, "--remote", "--content-type", "application/json"], { shell: true, maxBuffer: 1 << 24 })
  console.log(`DONE headshots ok=${ok} miss=${miss} err=${err} | credits + bio uploaded`)
} else {
  console.log("(dry) skipped fetch/upload")
}
