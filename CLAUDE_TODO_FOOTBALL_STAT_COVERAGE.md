# TODO — football blog-data stat coverage (why isn't it 100%?)

**Raised 2026-06 from inthegame-blog.** The blog's football Top Trumps card game
(`football/cards.qmd` + `football/cards-game.js`) shows per-player **counting stats**
(Goals, Assists, Shots, Key Passes, Passes, Tackles, Interceptions, Clearances) and
needs them for as many players as possible. Right now only **~66%** of rated players
have those stats, so the game has to filter the others out. This doc asks: *why isn't
coverage 100% across the board, and can we raise it?*

---

## What the game consumes (R2 file names)

All served from the `inthegame-data` R2 bucket, public base:
`https://pub-ee4bf5b599a047f9ac2b9facc1587008.r2.dev/football/`

| R2 file | What it has | Distinct rated players covered |
|---|---|---|
| `ratings.parquet` | Season Panna / Offense / Defense / SPM + EPV (`epv_*`) + WPA + PSV/OSV/DSV. **Model output.** | **6,721 (≈100%)** — the full rated pool |
| `game-logs.parquet` | **Per-match** Panna / EPV / WPA / PSV (model output). One row per player-match. | **5,585 (83%)** |
| `player-skills.parquet` | **Per-90 box-score counts** (`goals_p90`, `assists_p90`, `passes_p90`, `tackles_p90`, `interceptions_p90`, `clearances_p90`, `shots_p90`, …) **plus** skill ratings/accuracies. **Raw stats.** | **4,413 (66%)** |

The game derives season totals as `p90 × weighted_90s` from `player-skills.parquet`.
**That file is the ONLY source of real box-score counting stats** — `game-logs` and
`ratings` only carry the model values (EPV/WPA/Panna), not goals/assists/passes.

So the coverage of the concrete stats is bounded by `player-skills.parquet` (66%).

---

## The gap, by league (from a diagnostic over the live R2 files)

`rated players: 6,721 | have a player-skills row: 4,413 (66%) | have game-logs: 5,585 (83%)`

Per-league `with-skills / total` (worst offenders):
```
EPL                399/433     Serie_A   414/467    La_Liga 404/462   Bundesliga 346/399
Championship       534/612     Ligue_1   347/415    Eredivisie 340/397
Belgian_First_Division  40/361   ← very low
Conference_League  85/238      Ekstraklasa 39/70     Czech_Liga 43/72
"?" (unknown league)   0/457    ← NO skills
A_League           0/221       ← NO skills
CAF_CL             0/103       ← NO skills
```
Pattern: the **big leagues are partial** (some players in every covered league are
missing skills), and **whole competitions are absent** (A-League, CAF Champions
League, a 457-row "unknown league" bucket, and Belgian is oddly near-zero).

Reproduce: see `inthegame-blog` — I used a small hyparquet node script reading the
three R2 parquets and grouping by `league`.

---

## What I could trace in this repo (pannadata)

- `scripts/build_blog_data.R` builds **only `ratings.parquet`** (it says so on line ~177).
  It reads `source/seasonal_xrapm.parquet`, `source/seasonal_spm.parquet`,
  `source/player_metadata.parquet`, and aggregates per-season EPV/WPA/PSV from
  `blog/game-logs.parquet`. **It does not build `player-skills.parquet`.**
- `scripts/league_config.R` `BLOG_COMPS` lists only **13 competitions** (EPL,
  Championship, La_Liga, Ligue_1, Bundesliga, Serie_A, Eredivisie, Primeira_Liga,
  Scottish_Premiership, Super_Lig, UCL, UEL, Conference_League) — yet `ratings.parquet`
  contains far more leagues (A-League, Ekstraklasa, CAF_CL…). So `ratings`/the `source/*`
  files are built from a **broader** league set than `BLOG_COMPS`. Worth confirming which
  config actually governs each output.
- **`player-skills.parquet` is built in `.github/workflows/build-blog-data.yml`** (step
  "Download and filter player skills", ~line 250): it `gh release download opta-latest -p
  opta_skills.parquet`, then `read_parquet → filter(season_end_year == max(...)) →
  write_parquet("blog/player-skills.parquet")`. **The blog step only slices to the latest
  season — it applies NO league/minutes filter.** So whatever coverage `opta_skills.parquet`
  has *is* the ceiling.
- **`opta_skills.parquet` is the real source** — `DATA_DICTIONARY.md` / `README.md` describe
  it as *"Consolidated skills (from panna pipeline)"*, published to the `opta-latest` GitHub
  release. So the 66% cap is set **upstream in the `panna` package's Opta box-score
  consolidation**, not in pannadata. The trail leads out of this repo into `panna`.

---

## Questions to answer (the actual investigation — now in `panna`)

1. **In the `panna` pipeline, how is `opta_skills.parquet` built, and which competitions
   does it cover?** It's the consolidated Opta box-score per-90 feed. Find where it's
   assembled and what governs the league set.
2. **Why 66% when `ratings`/`game-logs` reach 83–100%?** Opta box-score (detailed event)
   data is licensed per competition — so the likely cause is **Opta simply doesn't cover
   A-League / CAF_CL / the "unknown league" bucket, and covers the big leagues only
   partially** (squad/minutes cutoffs). Confirm whether it's a hard licensing boundary vs.
   a filter we control (a minutes threshold, or comps we *could* ingest but don't).
   - Note: `ratings`/`game-logs` reach more players because the Panna **model** runs on a
     broader/lighter feed; the raw box-score counts need full Opta event data, which is narrower.
3. **Can box-score coverage be raised?** e.g. ingest more Opta comps into `opta_skills`,
   loosen a minutes cutoff, or back-fill goals/assists from **fbref/understat** (both already
   scraped in this repo — see the disabled `daily-fbref-scrape.yml` / `daily-understat-scrape.yml`)
   for competitions Opta doesn't cover.

---

## ✅ ANSWER (investigated in `panna`, 2026-06-05)

### 1. Where & how `opta_skills.parquet` is built

It's the output of the **estimated-skills pipeline** in the `panna` package
(`panna/data-raw/estimated-skills/`), NOT pannadata:

| Step | Script | What it does |
|---|---|---|
| 01 | `01_compute_match_stats.R` | Loads Opta **box-score** `player_stats` per league-season → per-match `_p90` rates (`compute_match_level_opta_stats()`). Source feed = `opta_player_stats.parquet` (via `load_opta_stats()` / RAPM cache `cache-opta/02_processed_data.rds`). |
| 02 | `02_estimate_skills.R` | `aggregate_skills_for_spm()` → one row per **player-season**, Bayesian decay-weighted skill estimates + the `_p90` counts. |
| 08 | `08_export_skills.R` | `write_parquet()` → `opta_skills.parquet`, `piggyback::pb_upload()` to the **`opta-latest`** release. |

The blog step in `build-blog-data.yml` then just downloads it and slices to the
latest season — confirmed: **no league/minutes filter downstream**, so the cap is
entirely upstream in steps 01/02.

### 2. What caps it at 66% — TWO compounding causes (both are filters WE control, NOT Opta licensing)

**CAP 1 — the league list (the big chunk).** The skills pipeline's `leagues`
vector is **15 comps**; the RAPM/ratings pipeline's is **20**. The 5-comp gap is
exactly the whole-competition zeros:

| Pipeline | `leagues` vector |
|---|---|
| **RAPM / `ratings.parquet`** (`run_pipeline_opta.R`) | ENG ESP GER ITA FRA NED POR TUR ENG2 SCO **BEL BRA AUS TUN CAFCL** UCL UEL UECL WC EURO |
| **Skills / `opta_skills.parquet`** (`run_skills_pipeline.R` + `01_compute_match_stats.R`) | ENG ESP GER ITA FRA NED POR TUR ENG2 SCO · · · · · UCL UEL UECL WC EURO |

Missing from skills = **BEL, BRA, AUS, TUN, CAFCL**. Maps (via `opta_loaders.R`
league codes) to: A_League (AUS) **0/221**, CAF_CL (CAFCL) **0/103**, Brazilian_Serie_A
(BRA, not in the blog breakdown but also 0), Tunisian_Ligue_1 (TUN), and
Belgian_First_Division (BEL) **40/361** — the 40 Belgians who score are the ones who
*also* appeared in UCL/UEL/UECL (which IS covered), so they leak in via the European
feed. The `"?" 0/457` unknown bucket and partial `Conference_League` are the same
shape: players whose only/primary competition isn't in the 15-list.

**This is NOT an Opta licensing boundary.** Proof: the **SPM half of the RAPM
pipeline already consumes Opta box-score `player_stats` for BEL/BRA/AUS/TUN/CAFCL**
— that's literally how those players get a `ratings.parquet` row. So
`opta_player_stats.parquet` *contains* the box-score counts for them; the skills
pipeline simply never reads those leagues. (The doc's hypothesis that "the model
runs on a broader/lighter feed" is half-right — the model pipeline runs on a
broader *league list*, but it's the **same box-score feed**, just more leagues.)

**CAP 2 — per-season minutes gate (the trim inside covered leagues).** In
`aggregate_skills_for_spm()` (`estimated_skills.R:885`): a player needs
**≥ 450 minutes in that season** (`min_minutes = 450`) plus `min_weighted_90s = 5`;
step 01 also drops player-matches under `min_match_minutes = 10`. Because the blog
slices to the **current season only**, this is what leaves EPL at 399/433,
Serie_A 414/467, etc. — fringe/low-minute squad players (cup cameos, late debuts,
injured) fall under 450 current-season minutes. This gate is deliberate: tiny
samples produce noisy per-90s, hence the Bayesian shrinkage + threshold.

### 3. Can coverage be raised? — YES

**Smallest, highest-leverage change (recovers the whole-competition zeros):**
add `"BEL", "BRA", "AUS", "TUN", "CAFCL"` to the skills `leagues` vector so it
matches the RAPM list, in **both**:
- `panna/data-raw/estimated-skills/run_skills_pipeline.R` (lines 18-23)
- `panna/data-raw/estimated-skills/01_compute_match_stats.R` (lines 18-23)

then re-run the skills pipeline steps 01-08 to regenerate + re-upload
`opta_skills.parquet`. **No new ingestion, no fbref/understat back-fill** — the
Opta box-score data already exists. This alone recovers A_League (~221), CAF_CL
(~103), Brazilian, Tunisian, and the bulk of Belgian — i.e. most of the 34% gap is
these absent competitions, not the minutes trim.

**Lower-leverage (optional):** loosen `min_minutes` / `min_weighted_90s` to pick up
fringe players inside covered leagues — but at the cost of noisier per-90 skill
estimates. Better left to the blog's own display filter than baked into the feed.

**fbref/understat back-fill: not needed.** The gap is leagues Opta *already covers*
but the skills pipeline omits. Back-fill would only matter for comps Opta genuinely
lacks box-scores for — none of the current gaps are that.

### ⚠ Operational gotcha found while tracing

**No scheduled CI job rebuilds `opta_skills.parquet`.** The only workflow that
sources `run_skills_pipeline.R` is `panna/.github/workflows/psr-weekly-snapshot.yml`,
and it runs with `start_step <- "8b"` (PSR-weekly export only — skips step 08).
`predictions-pipeline.yml` only *downloads/passes-through* the existing parquet.
So steps 01-08 (which build + upload the file) run **manually/locally** — the file
refreshes only on manual runs and the 15-league set is baked into whatever the last
manual run used. After any leagues-vector edit, someone must run steps 01-08 by hand
to regenerate and re-publish to `opta-latest`. (Worth filing as a follow-up: either
fold steps 01-08 into a scheduled workflow or add a `start_step <- 1` skills job.)

---

## ✅ UPDATE 2026-06-09 — fix shipped & verified, CAF/Tunisian decided, new carry-forward lever found

### Outcome: coverage 66% → **80%** (verified live on R2)

The leagues-vector fix shipped as `panna` commit `c6599e3` (2026-06-05), the skills
pipeline (steps 01-08) was re-run by hand that day (`cache-skills/` stamps 16:24-17:23
local), and the regenerated `opta_skills.parquet` re-published to `opta-latest`
(asset `updatedAt` 07:33 UTC = 17:33 AEST, right after step 07). `build-blog-data.yml`
then rebuilt the R2 file. Verified 2026-06-09 against the **live R2 parquets**:

| league | before | after |
|---|---|---|
| overall coverage | 4,413 / 6,721 = **66%** | 5,359 / 6,721 = **80%** |
| A_League | 0 / 221 | **166 / 217 (76%)** ✓ |
| Belgian_First_Division | 40 / 361 | **324 / 370 (88%)** ✓ |
| Brazilian_Serie_A | 0 | recovered ✓ |
| big-5 (EPL/Serie A/La Liga/Bund/L1) | ~88–94% | ~88–94% (minutes gate) |

### Big-league residual (the ~10% still missing in covered leagues) = the 450-min gate, working as designed

215 rated big-5 players lack a current-season skills row. They are **low-minute
fringe players** — median **362 min**, 75% under the 450-min gate (cup cameos,
January signings, injury-shortened seasons). Not a bug; loosening the gate trades
noise for coverage. Better handled by the carry-forward lever below.

### CAF_CL / Tunisian: NOT recoverable by the leagues-vector add — decided to blog-filter them out

`c6599e3` added `CAFCL`/`TUN` to the skills vector alongside `AUS`/`BEL`/`BRA`, but
unlike those three **CAF_CL stayed 0/103 and Tunisian 0/6** — the add was inert for
them. The source feed `opta_player_stats.parquet` *does* hold their box-scores
(CAF_CL 35,952 rows, Tunisian 60,859 rows, **including current season 2025-2026**),
so it's a skills-loader mapping gap, not missing data. **Decision (tier review): these
two are no longer treated as tier-2 for the blog, so rather than chase the loader bug
we exclude them at blog-build time.** Implemented 2026-06-09:
- `pannadata/scripts/league_config.R` — new `BLOG_COMP_EXCLUDE <- c("CAF_CL", "Tunisian_Ligue_1")`.
- `build_blog_data.R` — filters `enriched` on `BLOG_COMP_EXCLUDE` **before ranking**
  (so panna_rank/percentiles are over the blog pool); fan-out assertion adjusted to
  `n_before - n_excl`.
- `build_player_meta.R` — filters `player_meta` (drops players whose **main** comp is
  CAF/Tunisian; dual-comp players keep their domestic league).
They remain in the upstream ratings for other uses. (`game-logs.parquet` is a panna
step-10b pass-through and is **not** league-filtered here — a CAF/Tunisian player could
still appear in the Value tab; out of scope for this change.)

### 🆕 Biggest remaining lever: current-season-only slice discards every player's back-catalogue

Found while checking why F. Chiesa (rated, 637 min) has no skills card. The skills file
is **one row per player-season** (each season's values decay-weighted across recent
history via `weighted_90s`), but **the blog only serves the latest-season slice**.
Chiesa has 8 seasons of skills (2017–2024) sitting in `opta_skills.parquet` but **no
2025/2026 row** — his Liverpool minutes fell under the 450 gate, so the blog shows a
blank card despite a rich history.

Quantified across the whole pool (live R2, 2026-06-09):
- 1,362 rated players lack a current-season skills row.
- **604 of them have a prior qualifying season** (carry-forwardable, like Chiesa).
- 758 genuinely never had skills (true rookies / never-covered comps).
- **A "last-good-season carry-forward" would lift coverage 80% → 89%** — *without*
  loosening the 450-min noise gate. Big-5: 120 of the 215 missing return.

**Recommendation:** this is the highest-leverage next step. Implement as a blog-side
(or blog-build) fallback: when a rated player has no current-season skills row, use
their most recent qualifying season's `_p90`s (optionally flag as "stats from {season}").
Cleaner than loosening the gate, and surfaces marquee names with reduced current minutes.

---

## What "done" looks like

A short answer here (or in `DATA_DICTIONARY.md`) stating: how `opta_skills.parquet` is built
in `panna`, the exact reason coverage caps at 66% (Opta licensing vs. a filter we set), and
whether raising it is feasible (e.g. fbref/understat back-fill). If 66% is a hard Opta
ceiling, say so — the blog already filters games to covered players, so nothing breaks
meanwhile; this is purely an upside thread.

---

## Where the blog uses this (for context)

- `inthegame-blog/football/cards-game.js` — `DERIVED_TOTALS` maps display stat → `*_p90`
  source column; `STAT_PACKS` defines the editions.
- `inthegame-blog/football/cards.qmd` — `window._ensureSkills` lazy-loads
  `player-skills.parquet`, computes totals, and builds `window._deckStatsIds` (the set of
  players WITH stats). Games (Quick Play / Higher-Lower / Gaffer's Run) filter their pools
  to that set so they never deal an all-"—" card.
