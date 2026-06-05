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
//   plus: npm i --legacy-peer-deps smartcrop-sharp  (it pins an older sharp peer)
// Needs: football/player-bio.json on R2 (from build-football-bio.mjs); wrangler (R2 write).
// Run: node build-football-headshots.mjs            (full backfill)
//      node build-football-headshots.mjs --limit=40 (first N players, for testing)
//      node build-football-headshots.mjs --dry       (metadata only, no fetch/upload)
// TODO: fold into pannadata build-blog-data.yml to refresh with the data.

import sharp from 'sharp'
import { asyncBufferFromUrl, parquetReadObjects } from 'hyparquet'
import countries from 'i18n-iso-countries'
import en from 'i18n-iso-countries/langs/en.json' with { type: 'json' }
import smartcrop from 'smartcrop-sharp'
import * as tf from '@tensorflow/tfjs'
import { setWasmPaths } from '@tensorflow/tfjs-backend-wasm'
import * as faceapi from '@vladmandic/face-api/dist/face-api.node-wasm.js'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { writeFile, mkdir, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import os from 'node:os'
import path from 'node:path'
countries.registerLocale(en)
const exec = promisify(execFile)

// ── Face detection (wasm backend — no native binary, runs in CI + locally) ──
const require = createRequire(import.meta.url)
const faceApiDir = path.dirname(require.resolve('@vladmandic/face-api/package.json'))
const wasmDir = path.dirname(require.resolve('@tensorflow/tfjs-backend-wasm/package.json'))
async function initFace() {
  setWasmPaths(path.join(wasmDir, 'dist') + path.sep)
  await tf.setBackend('wasm'); await tf.ready()
  await faceapi.nets.ssdMobilenetv1.loadFromDisk(path.join(faceApiDir, 'model'))
  console.log('face model loaded · backend', tf.getBackend())
}
// Detect on a DOWNSCALED copy (≤1000px) — feeding a full-res Commons original
// (some are >100 megapixels) straight to the wasm tensor tries to allocate
// hundreds of MB and aborts the whole process (a wasm abort try/catch can't
// catch). Detection doesn't need full res; we scale the box back to original
// coordinates so the final crop is still cut from the high-res source.
async function faceBox(buf, meta) {
  try {
    const MAXD = 1000
    const scale = Math.min(1, MAXD / Math.max(meta.width || 1, meta.height || 1))
    const dw = Math.max(1, Math.round((meta.width || 1) * scale)), dh = Math.max(1, Math.round((meta.height || 1) * scale))
    const { data, info } = await sharp(buf).resize(dw, dh, { fit: "fill" }).removeAlpha().raw().toBuffer({ resolveWithObject: true })
    const t = tf.tensor3d(new Uint8Array(data), [info.height, info.width, 3])
    const d = await faceapi.detectAllFaces(t, new faceapi.SsdMobilenetv1Options({ minConfidence: 0.25 }))
    t.dispose()
    if (!d.length) return null
    const b = d.sort((a, b) => b.box.area - a.box.area)[0].box
    const inv = 1 / scale
    return { x: b.x * inv, y: b.y * inv, width: b.width * inv, height: b.height * inv }
  } catch { return null }
}

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
// Free = reuse-allowed (attribution OK) licenses. The Commons {{Attribution}}
// template, FAL and GFDL are all free but the old CC-only list skipped them —
// that's why e.g. Raphinha / Kenan Yıldız (both "Attribution"-licensed) showed
// as monograms. The NONFREE guard rejects NonCommercial / NoDerivs / fair-use
// even when the short name starts "CC BY" (the old `^cc[\- ]by` prefix wrongly
// accepted "CC BY-NC ..."). Free iff a free pattern matches AND no non-free one does.
const FREE = /^(cc0|cc[\- ]by|public domain|pd\b|pdm|attribution|fal\b|free art|gfdl)/i
const NONFREE = /(non[\- ]?commercial|no[\- ]?deriv|\bnc\b|\bnd\b|fair[\- ]?use|non[\- ]?free|all rights reserved)/i
const isFreeLicense = (...names) => names.some(n => FREE.test(n || "")) && !names.some(n => NONFREE.test(n || ""))

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

// ── 1) Wikidata: P18 (image) + P1532 (nation) + P31 (is it even a human?) ──
const byQid = new Map(players.map(p => [p.qid, p]))
const countryQids = new Set()
const claimIds = (c, prop) => (c[prop] || []).map(s => s.mainsnak?.datavalue?.value?.id).filter(Boolean)
for (const batch of chunk([...byQid.keys()], 50)) {
  const j = await wapi("www.wikidata.org", { action: "wbgetentities", ids: batch.join("|"), props: "claims" })
  for (const qid of batch) {
    const c = j.entities?.[qid]?.claims || {}
    const p = byQid.get(qid)
    p.img = c.P18?.[0]?.mainsnak?.datavalue?.value || null
    p.cfs = c.P1532?.[0]?.mainsnak?.datavalue?.value?.id || null
    p.human = claimIds(c, "P31").includes("Q5")    // P31 (instance of) = Q5 (human)
    if (p.cfs) countryQids.add(p.cfs)
  }
}
const withImg = players.filter(p => p.img).length, withCfs = players.filter(p => p.cfs).length
console.log(`P18 image: ${withImg} (${(100*withImg/players.length).toFixed(0)}%) | P1532 nation: ${withCfs} (${(100*withCfs/players.length).toFixed(0)}%)`)

// ── 1b) Re-resolve mis-links: when our QID points at a NON-human entity (e.g.
// reep linked "K. Mbappé" to the Wikidata item for the FC24 *video-game
// character*, which has no photo), search Wikidata by the player's name and
// adopt the real human footballer instead — but only with a strong guard:
// matching date-of-birth, or "human + footballer occupation". This is gated on
// !p.human so it stays a small set (the genuine mis-links), not every player
// who merely lacks a photo. ── DOB from reep (bio.dob) is the anti-homonym key.
let idToName = new Map()
try {
  const rr = await parquetReadObjects({ file: await asyncBufferFromUrl({ url: R2_PUB + "ratings.parquet?t=" + Math.floor(Math.random()*1e9) }) })
  for (const r of rr) if (r.player_id && !idToName.has(r.player_id)) idToName.set(r.player_id, r.player_name || "")
} catch (e) { console.warn("ratings load (for re-resolve names) failed:", e.message) }
const FOOTBALLER = "Q937857"   // association football player (P106 occupation)
async function reResolve(p) {
  const name = idToName.get(p.id); if (!name) return null
  const dob = p.bio && p.bio.dob ? p.bio.dob : null          // "YYYY-MM-DD"
  const surname = name.replace(/^\p{Lu}\.\s*/u, "").trim()    // strip a leading "K. "
  const terms = [...new Set([name, surname].filter(t => t && t.length > 1))]
  const cand = new Set()
  for (const t of terms) {
    const s = await wapi("www.wikidata.org", { action: "wbsearchentities", search: t, language: "en", type: "item", limit: "7" })
    for (const x of (s.search || [])) cand.add(x.id)
  }
  if (!cand.size) return null
  const j = await wapi("www.wikidata.org", { action: "wbgetentities", ids: [...cand].slice(0, 25).join("|"), props: "claims" })
  let footballerFallback = null
  for (const qid of cand) {
    const c = j.entities?.[qid]?.claims || {}
    if (!claimIds(c, "P31").includes("Q5")) continue          // must be human
    const img = c.P18?.[0]?.mainsnak?.datavalue?.value; if (!img) continue
    const cdob = c.P569?.[0]?.mainsnak?.datavalue?.value?.time // "+YYYY-MM-DD..."
    if (dob && cdob && cdob.slice(1, 11) === dob) return { qid, img }   // DOB match → certain
    if (!footballerFallback && claimIds(c, "P106").includes(FOOTBALLER)) footballerFallback = { qid, img }
  }
  return footballerFallback   // only used if no DOB match found
}
const mislinks = players.filter(p => !p.human && !p.img)
console.log(`non-human linked entities to re-resolve: ${mislinks.length}`)
let relinked = 0
for (const p of mislinks) {
  const r = await reResolve(p)
  if (r) { p.qid = r.qid; p.img = r.img; p.relinked = true; relinked++ }
  await new Promise(res => setTimeout(res, 120))   // polite to the Wikidata API
}
console.log(`re-resolved mis-links: ${relinked}/${mislinks.length} non-human linked entities now point at a real footballer with a photo`)

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
    const isFree = isFreeLicense(licCode, licShort)
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
  // persist a re-resolved QID so future runs start from the corrected link
  if (p.relinked) bioOut[p.id] = { ...bioOut[p.id], wd: p.qid }
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
      // Skip if BOTH variants already on R2 (resumable). --reprocess forces redo.
      if (!REPROCESS) {
        const [h1, h2] = await Promise.all([
          fetch(`${R2_PUB}headshots/${p.id}.webp`, { method: "HEAD" }).catch(() => null),
          fetch(`${R2_PUB}headshots-card/${p.id}.webp`, { method: "HEAD" }).catch(() => null),
        ])
        if (h1 && h1.ok && h2 && h2.ok) { credits[p.id] = p.credit; skipped++; ok++; return }
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
      const buf = Buffer.from(await res.arrayBuffer())
      const meta = await sharp(buf).metadata()

      // Both variants are built from the detected FACE BOX, which guarantees the
      // head is never clipped (smartcrop framed some players too low and cut heads):
      //   • CIRCLE avatar — tight: crop = 2.6× face height, ~0.55× headroom above.
      //   • CARD photo    — looser: crop = 3.9× face height, ~0.75× headroom → more
      //     shoulders/torso, reads like a trading card.
      // X is centred on the face; clamped to the source (narrow sources like an
      // 880px photo can't pull back further). No face found → smartcrop fallback.
      const box = await faceBox(buf, meta)
      const faceRegion = (scale, head) => {
        const S = Math.min(Math.round(box.height * scale), meta.width, meta.height)
        return {
          left: Math.max(0, Math.min(meta.width - S, Math.round(box.x + box.width / 2 - S / 2))),
          top: Math.max(0, Math.min(meta.height - S, Math.round(box.y - box.height * head))),
          width: S, height: S
        }
      }
      let circleRegion, cardRegion
      if (box) {
        circleRegion = faceRegion(2.6, 0.55)
        cardRegion = faceRegion(3.9, 0.75)
      } else {
        const c = (await smartcrop.crop(buf, { width: 320, height: 320, minScale: 1.0 })).topCrop
        circleRegion = cardRegion = { left: c.x, top: c.y, width: c.width, height: c.height }
      }
      const circleWebp = await sharp(buf).extract(circleRegion).resize(320, 320, { fit: "cover" }).webp({ quality: 82 }).toBuffer()
      const cardWebp = await sharp(buf).extract(cardRegion).resize(320, 320, { fit: "cover" }).webp({ quality: 82 }).toBuffer()

      const fp1 = path.join(TMP, `${p.id}.webp`), fp2 = path.join(TMP, `${p.id}-card.webp`)
      await writeFile(fp1, circleWebp); await writeFile(fp2, cardWebp)
      await exec(WRANGLER, ["r2", "object", "put", `inthegame-data/football/headshots/${p.id}.webp`, "--file", fp1, "--remote", "--content-type", "image/webp"], { shell: true, maxBuffer: 1 << 24 })
      await exec(WRANGLER, ["r2", "object", "put", `inthegame-data/football/headshots-card/${p.id}.webp`, "--file", fp2, "--remote", "--content-type", "image/webp"], { shell: true, maxBuffer: 1 << 24 })
      await rm(fp1).catch(() => {}); await rm(fp2).catch(() => {})
      credits[p.id] = p.credit
      ok++
    } catch (e) { err++; if (err <= 4) console.error(`err ${p.id}: ${String(e.message).slice(0, 120)}`) }
    finally { if (++done % 100 === 0) console.log(`  ${done}/${toFetch.length} ok=${ok}(skip ${skipped}) miss=${miss} err=${err}`) }
  }
  await initFace()
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
