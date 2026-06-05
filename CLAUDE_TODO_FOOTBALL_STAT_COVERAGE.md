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
