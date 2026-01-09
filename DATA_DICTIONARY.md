# pannadata Data Dictionary

This document describes all columns available in each table type stored in pannadata.

Data is sourced from FBref and stored as individual match RDS files at:
`data/{table_type}/{league}/{season}/{match_id}.rds`

Column names use snake_case (via `janitor::clean_names()`).

## Table Types

| Table Type | Description |
|------------|-------------|
| `summary` | Player match summary stats (goals, assists, xG, basic actions) |
| `passing` | Detailed passing statistics by distance and type |
| `defense` | Defensive actions (tackles, blocks, interceptions) |
| `possession` | Ball carrying, touches, and receiving stats |
| `shots` | Individual shot-level data with xG |
| `metadata` | Match metadata (teams, scores, date, URLs) |
| `keeper` | Goalkeeper-specific statistics |
| `misc` | Miscellaneous stats (fouls, aerials, recoveries) |
| `passing_types` | Pass type breakdown (live, dead, switches, etc.) |
| `fixtures` | Match schedule and results |

---

## summary

Player-level summary statistics per match. Primary table for basic stats.

| Column | Type | Description |
|--------|------|-------------|
| `player` | chr | Player name |
| `number` | int | Shirt number |
| `nation` | chr | Player nationality |
| `pos` | chr | Position (GK, DF, MF, FW) |
| `age` | chr | Player age |
| `min` | int | Minutes played |
| `gls` | int | Goals scored |
| `ast` | int | Assists |
| `pk` | int | Penalty kicks scored |
| `p_katt` | int | Penalty kicks attempted |
| `sh` | int | Total shots |
| `so_t` | int | Shots on target |
| `crd_y` | int | Yellow cards |
| `crd_r` | int | Red cards |
| `touches` | int | Total touches |
| `tkl` | int | Tackles |
| `int` | int | Interceptions |
| `blocks` | int | Blocks |
| `x_g` | num | Expected goals (xG) |
| `npx_g` | num | Non-penalty expected goals |
| `x_ag` | num | Expected assists (xA) |
| `sca` | int | Shot-creating actions |
| `gca` | int | Goal-creating actions |
| `cmp` | int | Passes completed |
| `att` | int | Passes attempted |
| `cmp_percent` | num | Pass completion % |
| `prg_p` | int | Progressive passes |
| `carries` | int | Carries |
| `prg_c` | int | Progressive carries |
| `att_2` | int | Take-ons attempted |
| `succ` | int | Successful take-ons |
| `team` | chr | Team name |
| `is_home` | lgl | Home team indicator |
| `match_url` | chr | FBref match URL |
| `league` | chr | League code (ENG, ESP, etc.) |
| `season` | chr | Season (e.g., "2024-2025") |

---

## passing

Detailed passing statistics with distance and accuracy breakdowns.

| Column | Type | Description |
|--------|------|-------------|
| **Totals** | | |
| `cmp` | int | Passes completed (total) |
| `att` | int | Passes attempted (total) |
| `cmp_percent` | num | Pass completion % (total) |
| `tot_dist` | int | Total passing distance (yards) |
| `prg_dist` | int | Progressive passing distance (yards) |
| **Short passes (<5 yds)** | | |
| `cmp_2` | int | Short passes completed |
| `att_2` | int | Short passes attempted |
| `cmp_percent_2` | num | Short pass completion % |
| **Medium passes (5-25 yds)** | | |
| `cmp_3` | int | Medium passes completed |
| `att_3` | int | Medium passes attempted |
| `cmp_percent_3` | num | Medium pass completion % |
| **Long passes (>25 yds)** | | |
| `cmp_4` | int | Long passes completed |
| `att_4` | int | Long passes attempted |
| `cmp_percent_4` | num | Long pass completion % |
| **Key passes** | | |
| `ast` | int | Assists |
| `x_ag` | num | Expected assists |
| `x_a` | num | xA (alternative column) |
| `kp` | int | Key passes (leading to shot) |
| `x1_3` | int | Passes into final third |
| `ppa` | int | Passes into penalty area |
| `crs_pa` | int | Crosses into penalty area |
| `prg_p` | int | Progressive passes |

---

## defense

Defensive action statistics.

| Column | Type | Description |
|--------|------|-------------|
| **Tackles** | | |
| `tkl` | int | Total tackles |
| `tkl_w` | int | Tackles won |
| `def_3rd` | int | Tackles in defensive third |
| `mid_3rd` | int | Tackles in middle third |
| `att_3rd` | int | Tackles in attacking third |
| **Challenges** | | |
| `tkl_2` | int | Dribblers tackled |
| `att` | int | Dribblers challenged |
| `tkl_percent` | num | % of dribblers tackled |
| `lost` | int | Challenges lost |
| **Blocks** | | |
| `blocks` | int | Total blocks |
| `sh` | int | Shots blocked |
| `pass` | int | Passes blocked |
| **Other** | | |
| `int` | int | Interceptions |
| `tkl_int` | int | Tackles + Interceptions |
| `clr` | int | Clearances |
| `err` | int | Errors leading to shot |

---

## possession

Ball carrying, touches, and receiving statistics.

| Column | Type | Description |
|--------|------|-------------|
| **Touches** | | |
| `touches` | int | Total touches |
| `def_pen` | int | Touches in defensive penalty area |
| `def_3rd` | int | Touches in defensive third |
| `mid_3rd` | int | Touches in middle third |
| `att_3rd` | int | Touches in attacking third |
| `att_pen` | int | Touches in attacking penalty area |
| `live` | int | Live-ball touches |
| **Take-ons (Dribbles)** | | |
| `att` | int | Take-ons attempted |
| `succ` | int | Successful take-ons |
| `succ_percent` | num | Take-on success % |
| `tkld` | int | Times tackled during take-on |
| `tkld_percent` | num | % tackled during take-on |
| **Carries** | | |
| `carries` | int | Total carries |
| `tot_dist` | int | Total carry distance (yards) |
| `prg_dist` | int | Progressive carry distance (yards) |
| `prg_c` | int | Progressive carries |
| `x1_3` | int | Carries into final third |
| `cpa` | int | Carries into penalty area |
| `mis` | int | Miscontrols |
| `dis` | int | Dispossessed |
| **Receiving** | | |
| `rec` | int | Passes received |
| `prg_r` | int | Progressive passes received |

---

## shots

Individual shot-level data.

| Column | Type | Description |
|--------|------|-------------|
| `player` | chr | Shooter name |
| `squad` | chr | Team name |
| `minute` | int | Minute of shot |
| `x_g` | num | Expected goals for this shot |
| `outcome` | chr | Shot outcome (Goal, Saved, etc.) |
| `distance` | int | Shot distance (yards) |
| `body_part` | chr | Body part used (Left, Right, Head) |
| `notes` | chr | Additional info (Penalty, etc.) |
| `match_url` | chr | FBref match URL |

---

## metadata

Match-level metadata.

| Column | Type | Description |
|--------|------|-------------|
| `match_url` | chr | FBref match URL (primary key) |
| `home_team` | chr | Home team name |
| `away_team` | chr | Away team name |
| `home_score` | int | Home team goals |
| `away_score` | int | Away team goals |
| `match_date` | date | Match date |
| `league` | chr | League code |
| `season` | chr | Season string |

---

## keeper

Goalkeeper-specific statistics.

| Column | Type | Description |
|--------|------|-------------|
| `so_ta` | int | Shots on target against |
| `ga` | int | Goals against |
| `saves` | int | Saves |
| `save_percent` | num | Save percentage |
| `ps_xg` | num | Post-shot xG (xG after shot taken) |
| `launch_percent_launched` | num | % of goal kicks launched |
| `avg_len_launched` | num | Avg length of goal kicks |
| `opp_opp` | int | Opponent passes faced |
| `stp_opp` | int | Opponent passes stopped |
| `stp_percent_opp` | num | % of opponent passes stopped |
| `att_sweeper` | int | Sweeper actions |
| `avg_dist_sweeper` | num | Avg sweeper action distance |

---

## misc

Miscellaneous statistics.

| Column | Type | Description |
|--------|------|-------------|
| `crd_y` | int | Yellow cards |
| `crd_r` | int | Red cards |
| `x2crd_y` | int | Second yellow cards |
| `fls` | int | Fouls committed |
| `fld` | int | Fouls drawn |
| `off` | int | Offsides |
| `crs` | int | Crosses |
| `tklw` | int | Tackles won |
| `pkwon` | int | Penalties won |
| `pkcon` | int | Penalties conceded |
| `og` | int | Own goals |
| `recov` | int | Ball recoveries |
| `won_aerial` | int | Aerials won |
| `lost_aerial` | int | Aerials lost |
| `won_percent_aerial` | num | Aerial duel win % |

---

## passing_types

Pass type breakdown.

| Column | Type | Description |
|--------|------|-------------|
| `att_pass_types` | int | Passes attempted |
| `live_pass_types` | int | Live-ball passes |
| `dead_pass_types` | int | Dead-ball passes |
| `fk_pass_types` | int | Free kick passes |
| `tb_pass_types` | int | Through balls |
| `sw_pass_types` | int | Switches |
| `crs_pass_types` | int | Crosses |
| `ti_pass_types` | int | Throw-ins |
| `ck_pass_types` | int | Corner kicks |
| `in_corner_kicks` | int | Inswinging corners |
| `out_corner_kicks` | int | Outswinging corners |
| `str_corner_kicks` | int | Straight corners |
| `cmp_outcomes` | int | Passes completed |
| `off_outcomes` | int | Offside passes |
| `blocks_outcomes` | int | Passes blocked |

---

## Common Identifiers

All tables include these columns for joining:

| Column | Description |
|--------|-------------|
| `match_url` | Unique FBref URL for the match |
| `league` | League code (ENG, ESP, GER, ITA, FRA, UCL, UEL) |
| `season` | Season string (e.g., "2024-2025") |
| `player` | Player name (for player-level tables) |
| `squad` / `team` | Team name |
| `is_home` / `home_away` | Home/away indicator |

---

## Notes

- Column names follow snake_case after `janitor::clean_names()`
- Passing distance columns use numbered suffixes: `_2` (short), `_3` (medium), `_4` (long)
- Some columns have the same name across tables with different meanings:
  - `att` in summary = passes attempted; in defense = dribblers challenged; in possession = take-ons attempted
  - `def_3rd`, `mid_3rd`, `att_3rd` appear in defense (tackles by zone) and possession (touches by zone)
- Missing data appears as NA
- Numeric columns may be stored as character and need conversion
