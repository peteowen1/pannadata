# TODO — Football `player-details.parquet` (analog of AFL `player-details`)

**Source**: Handoff from `inthegame-blog` session, 2026-05-18. Pete asked for
this; pick it up next time you're working in pannadata. Triggered by player
team/position bugs on `football/player-stats.qmd` (Van Dijk had no team,
Bruno Guimarães showed as "SUB").

## Why we need this

The blog currently resolves a football player's "current team" and "current
position" by walking `match-stats-{LEAGUE}.parquet` sorted by date desc and
keeping the first non-Substitute hit per `player_id`. This works (shipped
2026-05-18 in `football/player-stats.qmd`'s `_playerCurrentMeta` cell) but
has three real limitations:

1. **Single-league view is lossy.** When the user filters to one league
   (say ENG), only `match-stats-ENG.parquet` is loaded — a player whose
   most recent match was in another competition (e.g. UCL) doesn't get
   their newest team / league reflected. Affects players in mid-season
   transfer windows and players whose latest appearance is in cup
   competitions we don't track per-league box scores for.

2. **Computed client-side every page load.** Sorting ~120K matchStats rows
   on every render of `player-stats.qmd` is fine today but won't scale
   when we add similar logic to `player.qmd`, `team.qmd`, `compare.qmd`,
   and the matches-page card enrichment.

3. **`ratings.parquet.position` is all null.** The position fallback chain
   has to go through matchStats anyway because ratings carries no
   position data. A canonical side-table fixes both ratings and
   player-stats in one shot.

AFL solves this with `afl/player-details.parquet` (referenced by
`afl/player-stats.qmd:85` and called out in the blog's `CLAUDE.md` under
"Cricket parquet enrichment" / AFL stats data sources). Same pattern,
build a similar file for football.

## What to build

Emit `football/player-details.parquet` from `pannadata/build_blog_data.R`
(or wherever player-level joins land — check `R/` for the file that
currently writes `football/ratings.parquet`). One row per `player_id`,
recomputed nightly. **REEP-keyed** (use the same `player_id` that's
already on game-logs/match-stats/ratings — that's the cross-provider key
from `memory/reep_entity_register.md` in `~/.claude/projects/...inthegame-blog/memory/`).

Columns (mirror AFL's shape where it makes sense):

| column | type | notes |
|---|---|---|
| `player_id` | string | REEP id, primary key |
| `player_name` | string | display name (the one we want shown in tables) |
| `current_team` | string | latest team across ALL leagues (incl. UCL/UEL/cups) |
| `current_team_id` | string | for downstream joins |
| `current_league` | string | the league `current_team` plays its domestic matches in (ENG, ESP, …) — so when a player is on Real Madrid but last played in UCL, `current_league` is still ESP |
| `primary_position` | string | most common non-Substitute position across the last N matches (N=20 feels right — covers half a season) |
| `position_group` | string | one of GK/DEF/MID/FWD (use the same mapping the blog has in `football/football-maps.js` posToGroup so we don't drift) |
| `last_match_date` | date | for staleness checks |
| `nationality` | string | optional, nice-to-have |
| `dob` | date | optional |
| `height_cm`, `weight_kg` | numeric | optional |

## Derivation logic (R-ish pseudocode)

```r
# Pull all per-match player rows across every league we cover
all_matches <- bind_rows(
  read_parquet("match-stats-ENG.parquet"),
  read_parquet("match-stats-ESP.parquet"),
  ...
) |>
  arrange(desc(match_date))

# Latest team / league per player
team_state <- all_matches |>
  group_by(player_id) |>
  slice_head(n = 1) |>
  select(player_id, current_team = team_name, current_team_id = team_id,
         current_league = league, last_match_date = match_date)

# Primary position: modal non-Sub position from last 20 starts
position_state <- all_matches |>
  filter(!position %in% c("Substitute", "Sub")) |>
  group_by(player_id) |>
  slice_head(n = 20) |>
  count(player_id, position) |>
  slice_max(n, n = 1) |>
  select(player_id, primary_position = position)

# Join + add display name from latest row + position_group from posToGroup
player_details <- team_state |>
  left_join(position_state, by = "player_id") |>
  left_join(all_matches |> distinct(player_id, player_name), by = "player_id") |>
  mutate(position_group = pos_to_group[primary_position])
```

Save to R2 alongside the other blog parquets via the existing
`build-blog-data.yml` workflow.

## Blog-side cleanup (after this lands)

When this parquet ships, simplify `football/player-stats.qmd`:

- Delete `_playerCurrentMeta`, `_playerCurrentMetaByName`,
  `_playerTeamMap`, `_ratingsPositions` cells (~80 lines).
- Replace with one `_playerDetails = await window.fetchParquet(base +
  "football/player-details.parquet")` and a `Map(player_id → row)` from it.
- Both `tableData` branches (gameLogs + matchStats) just do
  `_playerDetailsMap.get(pid)?.current_team` / `.primary_position` —
  no aggregation, no fallback chains.

Also update `_quarto.yml` resources list and the R2 keys table in the blog
`CLAUDE.md` to include the new parquet.

## Verification once shipped

Open `dev.inthegame-blog.pages.dev/football/player-stats` and check:

1. **Van Dijk**: team = "Liverpool", position = "Centre-Back" (was: no team,
   currently fixed by the blog-side workaround but verify still works after
   parquet swap)
2. **Bruno Guimarães**: position = "Centre Midfielder" (or similar), NOT
   "SUB" — even when his most recent appearance was a cameo
3. **A player who just transferred mid-season** (e.g. anyone moving Jan
   2026 window): team should be the new club, not the old one
4. **A player on Real Madrid who only played UCL recently**: team should
   still show "Real Madrid" and `current_league` should be ESP

## Gotchas worth flagging

- `match-stats-{LEAGUE}` doesn't cover UCL/UEL/UECL — those parquets
  exist (UCL/UEL/UECL in the league codes list) and might have different
  schema. Make sure the bind_rows step handles missing columns or unions
  cleanly.
- Loan moves: a player on loan technically has a "parent club" and a
  "current club". The blog cares about current; that's what the latest
  match_date naturally gives us.
- New signings who haven't played yet: they'll be missing from
  `all_matches`. Either include them via a separate "registered roster"
  source, or accept they won't appear until their first match — same
  behaviour we have today.

— Generated from inthegame-blog session by Claude on 2026-05-18.
