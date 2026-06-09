// Build football/player-bio.json — per-player dob + nationality (+ flag ISO and
// Wikidata QID), keyed by Opta player_id. Source: the reep register (CC0) joined
// to our live ratings.parquet player_ids. The blog (football/player.qmd) fetches
// this once and shows age + a nationality flag on profiles and the Top Trumps card.
//
// CAVEAT: nationality comes from Wikidata "country of citizenship" (via reep), so
// multi-heritage players can show a heritage nation rather than their FIFA team
// (e.g. Olise → Nigeria, not France). Acceptable for v1; revisit if we find a
// "country for sport" field. Flag ISO is baked in here (i18n-iso-countries + a
// small override map for Wikidata's quirky labels) so the blog just renders it.
//
// Deps (install outside the repo): npm i hyparquet csv-parse i18n-iso-countries
// Needs reep people.csv (data/people.csv from github.com/withqwerty/reep) beside
// this script, and wrangler (R2 write) to upload the output.
// Run: node build-football-bio.mjs   (writes player-bio.json; upload via wrangler)
// TODO: move into pannadata's build-blog-data.yml so it refreshes with the data.

import { asyncBufferFromUrl, parquetReadObjects } from 'hyparquet'
import { parse } from 'csv-parse/sync'
import { readFileSync, writeFileSync } from 'node:fs'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'
import os from 'node:os'
import path from 'node:path'
import countries from 'i18n-iso-countries'
import en from 'i18n-iso-countries/langs/en.json' with { type: 'json' }
countries.registerLocale(en)
const exec = promisify(execFile)
// wrangler binary: CI (linux) has it on PATH as `wrangler`; local Windows uses
// the npm .cmd shim. Override with WRANGLER_BIN if needed.
const WRANGLER = process.env.WRANGLER_BIN ||
  (process.platform === "win32" ? path.join(os.homedir(), "AppData", "Roaming", "npm", "wrangler.cmd") : "wrangler")

// Wikidata label → [flagcdn ISO, display name]. Handles the quirky/football names
// i18n-iso-countries won't resolve. null ISO = no modern flag (skip).
const OVR = {
  "United Kingdom":["gb","United Kingdom"], "Kingdom of the Netherlands":["nl","Netherlands"],
  "Kingdom of Denmark":["dk","Denmark"], "Ivory Coast":["ci","Ivory Coast"],
  "Democratic Republic of the Congo":["cd","DR Congo"], "Republic of the Congo":["cg","Congo"],
  "Cape Verde":["cv","Cape Verde"], "South Korea":["kr","South Korea"], "North Macedonia":["mk","North Macedonia"],
  "The Gambia":["gm","Gambia"], "Bosnia and Herzegovina":["ba","Bosnia & Herzegovina"], "Iran":["ir","Iran"],
  "Russia":["ru","Russia"], "Kosovo":["xk","Kosovo"], "Czech Republic":["cz","Czechia"],
  "United States":["us","United States"], "South Korea ":["kr","South Korea"],
  "Federal Republic of Yugoslavia":[null,null], "Socialist Federal Republic of Yugoslavia":[null,null],
  "Soviet Union":[null,null], "Moldova":["md","Moldova"], "Syria":["sy","Syria"],
}
const toIso = (nat) => {
  if (!nat) return [null,null]
  if (OVR[nat]) return OVR[nat]
  const code = countries.getAlpha2Code(nat, "en")
  return code ? [code.toLowerCase(), nat] : [null, nat]
}

const f = await asyncBufferFromUrl({ url: "https://pub-ee4bf5b599a047f9ac2b9facc1587008.r2.dev/football/ratings.parquet?t="+Date.now() })
const rows = await parquetReadObjects({ file: f })
const ourIds = new Set(rows.map(r => r.player_id).filter(Boolean))
const recs = parse(readFileSync(new URL("./people.csv", import.meta.url)), { columns:true, relax_quotes:true, skip_records_with_error:true })

const bio = {}
let withDob=0, withIso=0, noIso=new Set()
for (const r of recs) {
  if (!r.key_opta || !ourIds.has(r.key_opta)) continue
  const [iso, natName] = toIso(r.nationality)
  if (r.nationality && !iso) noIso.add(r.nationality)
  const e = {}
  if (r.date_of_birth) { e.dob = r.date_of_birth; withDob++ }
  if (natName) e.nat = natName
  if (iso) { e.iso = iso; withIso++ }
  if (r.key_wikidata) e.wd = r.key_wikidata   // kept for step 3 (photos)
  if (Object.keys(e).length) bio[r.key_opta] = e
}
const outPath = fileURLToPath(new URL("./player-bio.json", import.meta.url))
writeFileSync(outPath, JSON.stringify(bio))
console.log(`bio entries: ${Object.keys(bio).length} | dob: ${withDob} | flag(iso): ${withIso}`)
console.log(`unmapped nationalities: ${[...noIso].join(", ") || "none"}`)

// Upload to R2 (skip with --no-upload for local dry runs). build-football-headshots.mjs
// reads this from R2 and overwrites it with P1532-sharpened nationality.
if (!process.argv.includes("--no-upload")) {
  await exec(WRANGLER, ["r2", "object", "put", "inthegame-data/football/player-bio.json", "--file", outPath, "--remote", "--content-type", "application/json"], { shell: true, maxBuffer: 1 << 24 })
  console.log("uploaded football/player-bio.json to R2")
}
