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
- **`player-skills.parquet`'s build was NOT found anywhere in pannaverse** — a repo-wide
  grep for `player-skills` / `player_skills` / `goals_p90` returned nothing in `.R`/`.yml`.
  So it's produced by something outside what's searchable here (the `panna` package? an
  Opta box-score script? a `source/*` parquet copied straight through?).

---

## Questions to answer (the actual investigation)

1. **Where is `player-skills.parquet` built and uploaded to R2?** Which script/workflow,
   and from which source feed (Opta box-score? fbref? understat?). Start from the
   `build-blog-data.yml` workflow and whatever produces the `source/*` skill parquet.
2. **Why 66% when `ratings` is ~100% and `game-logs` is 83%?** Specifically, is coverage
   limited by:
   - a **league filter** (only the Opta-licensed comps, vs. the broader set ratings uses)?
   - a **minutes / appearances threshold** dropping fringe players?
   - the box-score feed simply **not existing** for some comps (A-League, CAF_CL, the
     "unknown league" bucket)?
3. **Can it be raised toward `game-logs` (83%) or `ratings` (≈100%)?** e.g. by ingesting
   box-score counts for more competitions, loosening a minutes threshold, or back-filling
   from fbref/understat where Opta is absent.

## What "done" looks like

A short answer here (or in `DATA_DICTIONARY.md`) stating: where `player-skills.parquet`
comes from, the exact filter(s) that cap it at 66%, and whether raising coverage is
feasible/worthwhile. If it's feasible, a follow-up to extend the box-score stat build so
the blog's card game can include more leagues. If 66% is a hard Opta-licensing ceiling,
say so — then the blog will just keep filtering games to covered players (already does).

---

## Where the blog uses this (for context)

- `inthegame-blog/football/cards-game.js` — `DERIVED_TOTALS` maps display stat → `*_p90`
  source column; `STAT_PACKS` defines the editions.
- `inthegame-blog/football/cards.qmd` — `window._ensureSkills` lazy-loads
  `player-skills.parquet`, computes totals, and builds `window._deckStatsIds` (the set of
  players WITH stats). Games (Quick Play / Higher-Lower / Gaffer's Run) filter their pools
  to that set so they never deal an all-"—" card.
