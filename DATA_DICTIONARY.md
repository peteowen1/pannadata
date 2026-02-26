# pannadata Data Dictionary

This document describes the data stored in pannadata across all three sources.

**Opta** is the primary data source (15 leagues, 263 columns per player per match, event-level data with x/y coordinates). Understat and FBref provide supplementary coverage.

## Data Sources

| Source | Coverage | xG Model | Storage | Key Strength |
|--------|----------|----------|---------|--------------|
| **Opta** | 15 leagues, 2013+ | SPADL + XGBoost | Parquet per season, Parquet consolidated | 263 columns, event coordinates, progressive carries |
| **Understat** | Big 5 + Russia | Understat model | Parquet per season | xGChain, xGBuildup |
| **FBref** | Big 5 + cups | StatsBomb | RDS per match, Parquet per season | Detailed passing distance breakdowns |

## File Layout

```
data/
├── opta/                              # Primary source
│   ├── player_stats/{league}/{season}.parquet
│   ├── shots/{league}/{season}.parquet
│   ├── shot_events/{league}/{season}.parquet
│   ├── events/{league}/{season}.parquet
│   ├── match_events/{league}/{season}.parquet
│   ├── lineups/{league}/{season}.parquet
│   ├── fixtures/{league}/{season}.parquet
│   ├── xmetrics/{league}/{season}.parquet
│   ├── events_consolidated/           # Per-league consolidated events
│   │   └── events_{league}.parquet
│   ├── models/                        # Pre-trained ML models
│   │   ├── xg_model.rds
│   │   ├── xpass_model.rds
│   │   └── epv_model.rds
│   ├── opta_player_stats.parquet      # Consolidated (all leagues)
│   ├── opta_shots.parquet             # Consolidated
│   ├── opta_shot_events.parquet       # Consolidated
│   ├── opta_events.parquet            # Consolidated
│   ├── opta_lineups.parquet           # Consolidated
│   └── opta_fixtures.parquet          # Consolidated
├── understat/
│   ├── roster/{league}/{season}.parquet
│   ├── shots/{league}/{season}.parquet
│   ├── metadata/{league}/{season}.parquet
│   ├── understat_roster.parquet       # Consolidated
│   └── understat_shots.parquet        # Consolidated
└── fbref/
    ├── {table_type}/{league}/{season}/{match_id}.rds
    └── {table_type}/{league}/{season}.parquet
```

## League Codes

### Opta (15 leagues)

| League | Filesystem Code | R Alias | Seasons |
|--------|-----------------|---------|---------|
| Premier League | EPL | ENG | 2013+ |
| La Liga | La_Liga | ESP | 2013+ |
| Bundesliga | Bundesliga | GER | 2013+ |
| Serie A | Serie_A | ITA | 2013+ |
| Ligue 1 | Ligue_1 | FRA | 2013+ |
| Eredivisie | Eredivisie | NED | 2013+ |
| Primeira Liga | Primeira_Liga | POR | 2013+ |
| Super Lig | Super_Lig | TUR | 2013+ |
| Championship | Championship | ENG2 | 2013+ |
| Scottish Premiership | Scottish_Premiership | SCO | 2019+ |
| Champions League | UCL | UCL | 2013+ |
| Europa League | UEL | UEL | 2013+ |
| Conference League | Conference_League | UECL | 2021+ |
| World Cup | World_Cup | WC | 2014, 2018 |
| Euros | UEFA_Euros | EURO | 2016, 2024 |

> **Note:** "Filesystem Code" is used in data directory paths and `seasons.json`. "R Alias" is accepted by `panna` R package functions like `load_opta_stats("EPL", ...)`. Both work interchangeably in the R API.

### Understat

EPL, La_Liga, Bundesliga, Serie_A, Ligue_1, RFPL (Russia)

### FBref

ENG, ESP, GER, ITA, FRA (Big 5) + UCL, UEL, FA_CUP, EFL_CUP, etc.

## Format Conventions

- **Column names**: snake_case (via `janitor::clean_names()`)
- **Season format**: `"YYYY-YYYY"` for leagues (e.g., `"2024-2025"`), `"YYYY Country"` for tournaments (e.g., `"2018 Russia"`)
- **Match IDs**: Opta uses numeric IDs; FBref uses 8-char hex IDs (e.g., `74125d47`)
- **Missing data**: `NA`

---

# Opta Data Types

## opta/player_stats

Per-player-per-match statistics. **263 columns** covering every aspect of match performance. Key columns listed below — the full set is available via `load_opta_stats()`.

### Identifiers

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Opta match ID |
| `match_date` | date | Match date |
| `player_id` | int | Opta player ID |
| `player_name` | chr | Player name |
| `team_id` | int | Team ID |
| `team_name` | chr | Team name |
| `position` | chr | Position (Goalkeeper, Defender, Midfielder, Forward) |
| `mins_played` | int | Minutes played |

### Attacking — Shots & Goals

| Column | Type | Description |
|--------|------|-------------|
| `goals` | int | Total goals |
| `goals_openplay` | int | Open-play goals |
| `total_scoring_att` | int | Total shots |
| `attempts_ibox` | int | Shots inside box |
| `attempts_obox` | int | Shots outside box |
| `ontarget_scoring_att` | int | Shots on target |
| `shot_off_target` | int | Shots off target |
| `blocked_scoring_att` | int | Shots blocked |
| `att_hd_goal` | int | Headed goals |
| `att_rf_goal` | int | Right-foot goals |
| `att_lf_goal` | int | Left-foot goals |
| `att_pen_goal` | int | Penalty goals |
| `att_pen_miss` | int | Penalties missed |
| `hit_woodwork` | int | Hit woodwork |
| `big_chance_created` | int | Big chances created |
| `big_chance_scored` | int | Big chances scored |
| `big_chance_missed` | int | Big chances missed |

### Assisting

| Column | Type | Description |
|--------|------|-------------|
| `goal_assist` | int | Assists |
| `goal_assist_openplay` | int | Open-play assists |
| `goal_assist_setplay` | int | Set-piece assists |
| `second_goal_assist` | int | Second assists |
| `total_att_assist` | int | Key passes (total) |
| `ontarget_att_assist` | int | Key passes leading to shot on target |
| `put_through` | int | Through balls attempted |
| `successful_put_through` | int | Through balls completed |

### Passing

| Column | Type | Description |
|--------|------|-------------|
| `total_pass` | int | Total passes attempted |
| `accurate_pass` | int | Total passes completed |
| `total_final_third_passes` | int | Final third passes attempted |
| `successful_final_third_passes` | int | Final third passes completed |
| `total_long_balls` | int | Long balls attempted |
| `accurate_long_balls` | int | Long balls completed |
| `total_through_ball` | int | Through balls attempted |
| `accurate_through_ball` | int | Through balls completed |
| `total_cross` | int | Crosses attempted |
| `accurate_cross` | int | Crosses completed |
| `total_cross_nocorner` | int | Open-play crosses attempted |
| `accurate_cross_nocorner` | int | Open-play crosses completed |
| `backward_pass` | int | Backward passes |
| `fwd_pass` | int | Forward passes |
| `total_layoffs` | int | Layoffs attempted |
| `accurate_layoffs` | int | Layoffs completed |

### Defending

| Column | Type | Description |
|--------|------|-------------|
| `total_tackle` | int | Total tackles |
| `won_tackle` | int | Tackles won |
| `interception` | int | Interceptions |
| `interception_won` | int | Interceptions won |
| `interceptions_in_box` | int | Interceptions in box |
| `outfielder_block` | int | Blocks |
| `blocked_pass` | int | Passes blocked |
| `blocked_cross` | int | Crosses blocked |
| `duel_won` | int | Duels won |
| `duel_lost` | int | Duels lost |
| `aerial_won` | int | Aerials won |
| `aerial_lost` | int | Aerials lost |
| `last_man_tackle` | int | Last-man tackles |
| `six_yard_block` | int | Six-yard blocks |
| `challenge_lost` | int | Challenges lost |

### Clearances

| Column | Type | Description |
|--------|------|-------------|
| `total_clearance` | int | Total clearances |
| `effective_clearance` | int | Effective clearances |
| `head_clearance` | int | Headed clearances |
| `clearance_off_line` | int | Goal-line clearances |

### Possession & Ball Control

| Column | Type | Description |
|--------|------|-------------|
| `touches` | int | Total touches |
| `touches_in_opp_box` | int | Touches in opponent's box |
| `poss_won_def3rd` | int | Possession won in defensive third |
| `poss_won_mid3rd` | int | Possession won in middle third |
| `poss_won_att3rd` | int | Possession won in attacking third |
| `ball_recovery` | int | Ball recoveries |
| `dispossessed` | int | Times dispossessed |
| `turnover` | int | Turnovers |
| `poss_lost_all` | int | Total possessions lost |
| `poss_lost_ctrl` | int | Possessions lost under control |
| `unsuccessful_touch` | int | Unsuccessful touches |
| `carries` | int | Carries |
| `progressive_carries` | int | Progressive carries |
| `pen_area_entries` | int | Penalty area entries |
| `final_third_entries` | int | Final third entries |

### Set Pieces

| Column | Type | Description |
|--------|------|-------------|
| `corner_taken` | int | Corners taken |
| `won_corners` | int | Corners won |
| `total_corners_intobox` | int | Corners into box attempted |
| `accurate_corners_intobox` | int | Corners into box accurate |
| `freekick_cross` | int | Free kick crosses |
| `total_pull_back` | int | Pull-backs attempted |
| `accurate_pull_back` | int | Pull-backs completed |
| `total_flick_on` | int | Flick-ons attempted |
| `accurate_flick_on` | int | Flick-ons completed |

### Goalkeeper

| Column | Type | Description |
|--------|------|-------------|
| `saves` | int | Total saves |
| `saved_ibox` | int | Saves inside box |
| `saved_obox` | int | Saves outside box |
| `goals_conceded` | int | Goals conceded |
| `goals_conceded_ibox` | int | Goals conceded inside box |
| `total_high_claim` | int | High claims attempted |
| `good_high_claim` | int | High claims won |
| `punches` | int | Punches |
| `keeper_throws` | int | Throws |
| `accurate_keeper_throws` | int | Accurate throws |
| `gk_smother` | int | Smothers |
| `total_keeper_sweeper` | int | Sweeper actions |
| `accurate_keeper_sweeper` | int | Accurate sweeper actions |

### Fouls & Cards

| Column | Type | Description |
|--------|------|-------------|
| `fouls` | int | Fouls committed |
| `was_fouled` | int | Fouls drawn |
| `fouled_final_third` | int | Fouled in final third |
| `yellow_card` | int | Yellow cards |
| `red_card` | int | Red cards |
| `second_yellow` | int | Second yellow cards |
| `penalty_won` | int | Penalties won |
| `penalty_conceded` | int | Penalties conceded |
| `offsides` | int | Offsides |
| `offside_provoked` | int | Offsides provoked |
| `error_lead_to_shot` | int | Errors leading to shot |
| `error_lead_to_goal` | int | Errors leading to goal |

---

## opta/shot_events

Individual shot-level data with x/y coordinates. Used for xG model training.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Opta match ID |
| `event_id` | int | Event ID |
| `player_id` | int | Shooter ID |
| `player_name` | chr | Shooter name |
| `team_id` | int | Team ID |
| `minute` | int | Minute of shot |
| `second` | int | Second of shot |
| `x` | num | Shot x-coordinate (0-100 scale) |
| `y` | num | Shot y-coordinate (0-100 scale) |
| `outcome` | int | 1 = on target, 0 = off target |
| `is_goal` | lgl | Whether the shot was a goal |
| `type_id` | int | Opta type (13=miss, 14=post, 15=saved, 16=goal) |
| `body_part` | chr | Head, LeftFoot, RightFoot |
| `situation` | chr | OpenPlay, SetPiece, Corner, Penalty |
| `big_chance` | lgl | Big chance flag |

---

## opta/match_events

All in-match events with x/y coordinates (~2000 events per match). Used for SPADL conversion, EPV pipeline, and xMetrics calculation.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Opta match ID |
| `event_id` | int | Event ID |
| `type_id` | int | Opta event type (1=pass, 3=dribble, 7=tackle, 13=miss, 14=post, 15=saved, 16=goal, 44=aerial, etc.) |
| `player_id` | int | Player ID |
| `player_name` | chr | Player name |
| `team_id` | int | Team ID |
| `minute` | int | Minute |
| `second` | int | Second |
| `x` | num | Start x-coordinate (0-100 scale) |
| `y` | num | Start y-coordinate (0-100 scale) |
| `end_x` | num | End x-coordinate (for passes/carries) |
| `end_y` | num | End y-coordinate (for passes/carries) |
| `outcome` | int | 1 = successful, 0 = unsuccessful |
| `period_id` | int | 1 = first half, 2 = second half |
| `qualifier_json` | chr | Full qualifier data as JSON string |

---

## opta/events

High-level match events: goals, cards, substitutions.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Opta match ID |
| `match_date` | date | Match date |
| `event_type` | chr | goal, yellow_card, red_card, substitution |
| `minute` | int | Minute |
| `second` | int | Second |
| `team_id` | int | Team ID |
| `player_id` | int | Player ID |
| `player_name` | chr | Player name |
| `assist_player_id` | int | Assist provider ID (goals only) |
| `assist_player_name` | chr | Assist provider name (goals only) |
| `player_on_id` | int | Substitute coming on (subs only) |
| `player_on_name` | chr | Substitute coming on name (subs only) |
| `player_off_id` | int | Player coming off (subs only) |
| `player_off_name` | chr | Player coming off name (subs only) |

---

## opta/lineups

Starting XI, substitutions, positions, and minutes played.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Opta match ID |
| `match_date` | date | Match date |
| `player_id` | int | Player ID |
| `player_name` | chr | Player name |
| `team_id` | int | Team ID |
| `team_name` | chr | Team name |
| `team_position` | chr | home / away |
| `position` | chr | Goalkeeper, Defender, Midfielder, Forward |
| `position_side` | chr | Left, Right, Centre |
| `formation_place` | chr | 1-11 for starters, NA for subs |
| `shirt_number` | int | Shirt number |
| `is_starter` | lgl | Whether player started |
| `minutes_played` | int | Minutes played |
| `sub_on_minute` | int | Substitution on minute (0 if starter) |
| `sub_off_minute` | int | Substitution off minute (0 if played full match) |

**Note:** Lineups have no score columns. Derive match scores from goal events.

---

## opta/fixtures

Match schedule and results. Contains ALL statuses (Played, Fixture, Postponed).

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Opta match ID |
| `match_date` | date | Match date |
| `home_team` | chr | Home team name |
| `away_team` | chr | Away team name |
| `home_team_id` | int | Home team ID |
| `away_team_id` | int | Away team ID |
| `home_score` | int | Full-time home goals (NA for non-played matches) |
| `away_score` | int | Full-time away goals (NA for non-played matches) |
| `home_score_ht` | int | Half-time home goals (NA for non-played matches) |
| `away_score_ht` | int | Half-time away goals (NA for non-played matches) |
| `match_status` | chr | Fixture, Played, Postponed |
| `competition` | chr | Opta league code |
| `season` | chr | Season string |

Filter with `load_opta_fixtures(league, status = "Fixture")` for upcoming matches.

---

## opta/xmetrics

Pre-computed xG, xA, and xPass metrics per player per match. Generated by the SPADL + XGBoost pipeline from match_events data.

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | int | Opta player ID |
| `player_name` | chr | Player name |
| `team_name` | chr | Team name |
| `match_id` | int | Opta match ID |
| `minutes` | int | Minutes played |
| `shots` | int | Total shots |
| `shots_on_target` | int | Shots on target |
| `goals` | int | Goals scored |
| `npgoals` | int | Non-penalty goals |
| `xg` | num | Expected goals (XGBoost model) |
| `npxg` | num | Non-penalty xG |
| `key_passes` | int | Key passes |
| `assists` | int | Assists |
| `xa` | num | Expected assists (xPass model) |
| `passes_attempted` | int | Passes attempted |
| `passes_completed` | int | Passes completed |
| `sum_xpass` | num | Sum of pass completion probabilities |
| `xpass_overperformance` | num | Actual completions minus expected |

**Note:** Penalty xG is overridden to 0.76 in the `xg` column (model was not trained on penalties). Penalty shots are excluded from `npxg`.

---

## opta/shots

Aggregated shot data per player per match (summary-level, not event-level). For individual shots with coordinates, use `shot_events`.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Opta match ID |
| `player_id` | int | Player ID |
| `player_name` | chr | Player name |
| `team_id` | int | Team ID |
| `team_name` | chr | Team name |
| `position` | chr | Position |
| `minutes_played` | int | Minutes played |
| `total_shots` | int | Total shots |
| `shots_on_target` | int | Shots on target |
| `shots_off_target` | int | Shots off target |
| `shots_blocked` | int | Shots blocked |
| `shots_inside_box` | int | Shots inside box |
| `shots_outside_box` | int | Shots outside box |
| `shots_right_foot` | int | Right-foot shots |
| `shots_left_foot` | int | Left-foot shots |
| `shots_header` | int | Headed shots |
| `goals` | int | Goals scored |
| `goals_inside_box` | int | Goals from inside box |
| `shots_open_play` | int | Open-play shots |
| `shots_corner` | int | Shots from corners |
| `shots_penalty` | int | Penalty shots |
| `big_chance_created` | int | Big chances created |
| `big_chance_missed` | int | Big chances missed |
| `big_chance_scored` | int | Big chances scored |

---

## opta/models

Pre-trained XGBoost models stored at `data/opta/models/`.

| File | Description |
|------|-------------|
| `xg_model.rds` | Expected goals model (trained on SPADL shot features) |
| `xpass_model.rds` | Pass completion probability model |
| `epv_model.rds` | Expected possession value model |

---

# Understat Data Types

## understat/roster

Player-level stats per season. Unique for xGChain and xGBuildup metrics.

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | int | Understat player ID |
| `player_name` | chr | Player name |
| `team_title` | chr | Team name |
| `position` | chr | Position |
| `games` | int | Games played |
| `time` | int | Minutes played |
| `goals` | int | Goals |
| `assists` | int | Assists |
| `shots` | int | Total shots |
| `key_passes` | int | Key passes |
| `yellow_cards` | int | Yellow cards |
| `red_cards` | int | Red cards |
| `xg` | num | Expected goals |
| `xa` | num | Expected assists |
| `xg_chain` | num | xG chain (involvement in possessions ending in shot) |
| `xg_buildup` | num | xG buildup (involvement minus final action) |
| `npg` | int | Non-penalty goals |
| `npxg` | num | Non-penalty xG |
| `xg_per_avg` | num | xG per 90 |
| `xa_per_avg` | num | xA per 90 |

## understat/shots

Individual shot-level data from Understat.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | Shot ID |
| `player_id` | int | Shooter ID |
| `player` | chr | Shooter name |
| `minute` | int | Minute of shot |
| `x` | num | Shot x-coordinate (0-1 scale) |
| `y` | num | Shot y-coordinate (0-1 scale) |
| `xg` | num | Expected goals |
| `result` | chr | Shot outcome |
| `situation` | chr | Open play, set piece, etc. |
| `shot_type` | chr | Body part |
| `match_id` | int | Match ID |
| `h_team` | chr | Home team |
| `a_team` | chr | Away team |

## understat/metadata

Match-level metadata from Understat.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | int | Understat match ID |
| `home_team` | chr | Home team |
| `away_team` | chr | Away team |
| `home_goals` | int | Home goals |
| `away_goals` | int | Away goals |
| `home_xg` | num | Home team xG |
| `away_xg` | num | Away team xG |
| `date` | date | Match date |
| `league` | chr | League code |
| `season` | chr | Season (e.g., "2024") |

---

# FBref Data Types

FBref data is stored as individual match RDS files at `data/fbref/{table_type}/{league}/{season}/{match_id}.rds`. Column names use snake_case (via `janitor::clean_names()`).

## FBref Table Types

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

## fbref/summary

Player-level summary statistics per match.

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

## fbref/passing

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

## fbref/defense

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

## fbref/possession

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

## fbref/shots

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

## fbref/metadata

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

## fbref/keeper

Goalkeeper-specific statistics.

| Column | Type | Description |
|--------|------|-------------|
| `so_ta` | int | Shots on target against |
| `ga` | int | Goals against |
| `saves` | int | Saves |
| `save_percent` | num | Save percentage |
| `ps_xg` | num | Post-shot xG |
| `launch_percent_launched` | num | % of goal kicks launched |
| `avg_len_launched` | num | Avg length of goal kicks |
| `opp_opp` | int | Opponent passes faced |
| `stp_opp` | int | Opponent passes stopped |
| `stp_percent_opp` | num | % of opponent passes stopped |
| `att_sweeper` | int | Sweeper actions |
| `avg_dist_sweeper` | num | Avg sweeper action distance |

## fbref/misc

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

## fbref/passing_types

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

## FBref Common Identifiers

All FBref tables include these columns for joining:

| Column | Description |
|--------|-------------|
| `match_url` | Unique FBref URL for the match |
| `league` | League code (ENG, ESP, GER, ITA, FRA, UCL, UEL) |
| `season` | Season string (e.g., "2024-2025") |
| `player` | Player name (for player-level tables) |
| `squad` / `team` | Team name |
| `is_home` / `home_away` | Home/away indicator |

## FBref Notes

- Passing distance columns use numbered suffixes: `_2` (short), `_3` (medium), `_4` (long)
- Some columns share names across tables with different meanings:
  - `att` in summary = passes attempted; in defense = dribblers challenged; in possession = take-ons attempted
  - `def_3rd`, `mid_3rd`, `att_3rd` appear in defense (tackles by zone) and possession (touches by zone)
- Numeric columns may be stored as character and need conversion
