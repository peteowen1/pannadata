# `player-positions.parquet` — derived detailed positions from chain x/y

> Filed from an inthegame-blog session 2026-05-29. The blog wants a Match-Pos
> filter that distinguishes LB / CB / RB / LM / CM / RM / LW / RW, but Opta's
> raw `position` field across `match-stats`, `chains`, `player-details`, and
> `ratings` only emits **8 distinct values** (verified by `distinct()` across
> all four sources):
>
> - Goalkeeper / Defender / Wing Back / Defensive Midfielder / Midfielder /
>   Attacking Midfielder / Striker / Substitute (+ null)
>
> Even known fullbacks (L. Shaw / K. Trippier / Pedro Porro / A. Robertson)
> are all just "Defender" with no left/right/centre split. Same for
> wide midfielders / wingers.
>
> Lifting the derivation upstream so the blog can consume a small parquet
> instead of loading the full chains file (~615K rows for EPL alone) just to
> bucket players.

## Output schema

`player-positions.parquet` on the `inthegame-data` R2 bucket under `football/` (mirrors the existing `game_logs.parquet` / `match-stats-{CODE}.parquet` upload pattern in `build-blog-data.yml`).

| Column | Type | Notes |
|--------|------|-------|
| `player_id` | str (Opta) | join key with all other blog parquets |
| `player_name` | str | for human-readable verification |
| `league` | str | short code (ENG, ESP, ITA, GER, FRA, NED, POR, SCO, TUR, ENG2) |
| `season` | str | e.g. `"2025-2026"` |
| `opta_position` | str | mode of Opta-emitted `position` across the player's matches in this season |
| `detailed_position` | str | derived: `GK` / `LB` / `CB` / `RB` / `WB` / `DM` / `CM` / `AM` / `LM` / `RM` / `LW` / `RW` / `ST` |
| `avg_x` | float | per-player-season mean of chain touch x |
| `avg_y` | float | per-player-season mean of chain touch y |
| `n_touches` | int | sample size; nullable / drop `detailed_position` when too small to derive confidently (suggested threshold: 100) |

Sizing: ~1k players × ~10 leagues × ~16 seasons ≈ <200k rows. Few MB parquet.

## Suggested v1 derivation algorithm

Bands open to iteration — easier to validate visually in R with proper overlays than client-side in OJS.

```r
# Inputs:
#   chains: chains-{CODE}.parquet rows (need `player_id`, `season`, `x`, `y`)
#   match_stats: for opta_position mode per (player_id, season)
#
# Opta y-axis convention (verified per CLAUDE.md in inthegame-blog):
#   y = 0    → attacker's right
#   y = 100  → attacker's left
#   y = 50   → centre
#
# So a left-footed attacker on the left wing has avg_y > 50.

classify_detailed <- function(opta_pos, avg_y) {
  if (is.na(opta_pos) || opta_pos == "Substitute") return(NA_character_)

  if (opta_pos == "Defender") {
    if (avg_y > 67) return("LB")
    if (avg_y < 33) return("RB")
    return("CB")
  }
  if (opta_pos == "Midfielder") {
    if (avg_y > 67) return("LM")
    if (avg_y < 33) return("RM")
    return("CM")
  }
  if (opta_pos == "Striker") {
    if (avg_y > 67) return("LW")
    if (avg_y < 33) return("RW")
    return("ST")
  }
  # GK / WB / DM / AM pass through — Opta already distinguishes these
  c("Goalkeeper"="GK", "Wing Back"="WB",
    "Defensive Midfielder"="DM", "Attacking Midfielder"="AM")[opta_pos]
}
```

## Algorithm options to evaluate

1. **Simple x/y banding (above)** — fast, transparent, but 33/67 cutoffs are arbitrary
2. **Per-team relative** — subtract team mean y before bucketing (handles teams that play asymmetric formations)
3. **K-means on (x, y)** — let the data find natural clusters per opta_position
4. **Mode of per-match centroids** — compute one centroid per game played, take the mode across the season (more robust to occasional out-of-position appearances)
5. **Two-stage: classify then re-train** — use simple bucketing as labels for a learned classifier on additional features (touches by zone, pass directions, etc.)

Worth visualising on a few known players (L. Shaw should be LB, T. Alexander-Arnold should be RB, V. van Dijk should be CB, M. Salah should be RW, etc.) before locking the algorithm in.

## Pipeline placement

Suggested fold into existing `build-blog-data.yml` — runs after `build_chains_ci.R` (which already loads all chain shards), shares its in-memory chains data, writes a small additional parquet. New step around line 365 of the workflow.

Trigger automatically with the rest of the blog data via `repository_dispatch: predictions-complete`.

## Blog consumer side (already prepped)

Blog has been trimmed to only show pills for the 7 panna codes Opta actually emits (commits `611af0e` + `791ab08` on `dev`). Once this parquet lands, the blog will:

1. Lazy-load `player-positions.parquet` only when the Match Pos filter is touched
2. Expand `detailedPosCodes` in `football/football-maps.js` from 7 → 13 (add LB/CB/RB/LM/CM/RM/LW/RW)
3. Switch the Match Pos filter from `posToDetailed[d.position]` (one-shot Opta lookup) to a `player_id → detailed_position` lookup from this parquet

Tracked at inthegame-blog#257.

## Out of scope

- Detailed positions for cup competitions (UCL / UEL / UECL / WC) — players have too few games for stable derivation; pass through their domestic-league assignment if available
- Multi-position players within a season — emit one row per player-season, take primary; could later split into multiple rows if useful
