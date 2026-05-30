#!/usr/bin/env Rscript
# Build player-positions.parquet — v2.1 per-match hybrid.
#
# Three detailed-position derivations per (player_id, league, season):
#   opta_detailed_position  — Opta `position` + `position_side` per appearance,
#                             moded over the player's sided STARTS. Opta's own
#                             formation tags (Left / Left-Centre / Centre /
#                             Centre-Right / Right) — ground truth, no inference.
#   panna_detailed_position — chain x/y touch-centroid per MATCH, classified by
#                             avg_y bands + avg_x full-back gate, moded over the
#                             season. Panna's inferred view (no Opta side tag).
#   detailed_position       — HYBRID: Opta side when confident (>= MIN_STARTS
#                             sided starts), else fall back to panna's centroid.
#
# Why per-match + mode (not season-pooled mean): a CB who covers LB for 2 games
# keeps his CB label — the 2 LB matches lose the mode. Season-pooled means blend
# the two roles into one drifting centroid that lands between them.
#
# Why expose both sources: Opta tags are ground truth but occasionally off (e.g.
# a LB modally tagged Left-Centre); the panna centroid is a useful independent
# cross-check, and downstream (blog player profiles) can show "Opta vs panna".
#
# Reads:
#   source/opta_player_stats.parquet  (per-appearance: position, position_side,
#                                       gameStarted, match_id)
#   blog/chains-{CODE}.parquet         (per-touch: player_id, match_id, x, y)
#
# Writes:
#   blog/player-positions.parquet
#
# Schema:
#   player_id, player_name, league, season, opta_position,
#   opta_detailed_position, panna_detailed_position, detailed_position,
#   pos_source, pos_agreement, n_starts, avg_x, avg_y, sd_x, sd_y, n_touches

suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(data.table)
})

src_dir <- "source"
blog_dir <- "blog"
out_path <- file.path(blog_dir, "player-positions.parquet")
dir.create(blog_dir, showWarnings = FALSE)

cat("=== Player Positions Builder (v2.1 per-match hybrid) ===\n")

# Opta competition code -> blog short code (matches build_chains_ci.R output)
COMP_TO_CODE <- c(
  EPL = "ENG", La_Liga = "ESP", Serie_A = "ITA", Bundesliga = "GER",
  Ligue_1 = "FRA", Eredivisie = "NED", Primeira_Liga = "POR",
  Super_Lig = "TUR", Championship = "ENG2",
  Scottish_Premiership = "SCO"
)

MATCH_TOUCH_MIN <- 25L    # per-match touch floor for a trusted centroid
MIN_STARTS      <- 3L     # sided starts needed for a stable Opta mode
TOUCH_THRESHOLD <- 100L   # season-level aux touch floor (avg_x/avg_y/sd)
REAL_SIDES <- c("Left", "Left/Centre", "Centre", "Centre/Right", "Right")

# --- mode helper: most-frequent non-NA value; ties broken by alphabetical ----
mode_chr <- function(x) {
  x <- x[!is.na(x)]
  if (!length(x)) return(NA_character_)
  tab <- table(x)
  names(tab)[order(-tab, names(tab))][1L]
}

# --- Opta (position + position_side + formation) -> detailed, per appearance -
# The wide/central cut differs by line — validated against EPL formation×side:
#   Defender    : only pure Left/Right is wide (LB/RB); any "Centre" side is a
#                 centre-back — keeps a Left/Centre LCB as CB (fixes Saliba /
#                 Guéhi / van de Ven) and a front-two striker out of the wing.
#   Midfielder  : same — pure Left/Right = wide mid (LM/RM); a Left/Centre mid
#                 is a central midfielder (CM), not LM.
#   Att. Mid    : ANY lateral side is wide — the wide "2" of a 3-4-2-1 tag
#                 Left/Centre·Centre/Right, the "3" of a 4-2-3-1 tag Left/Right.
#                 A central AM is only ever pure Centre. (AMs are never paired
#                 strikers, so widening is safe here.)
#   Striker     : formation-dependent — the last digit of team_formation is the
#                 number of forwards. In a 3+-forward shape (433, 343) the wide
#                 forwards are wingers; in a front two (442, 352, 532) both are
#                 strikers; a lone striker (4231, 4141) is always pure Centre.
opta_detail <- function(position, side, formation) {
  n_fwd    <- suppressWarnings(as.integer(substr(formation, nchar(formation),
                                                 nchar(formation))))
  is_left  <- !is.na(side) & side == "Left"
  is_right <- !is.na(side) & side == "Right"
  am_left  <- !is.na(side) & side %in% c("Left", "Left/Centre")
  am_right <- !is.na(side) & side %in% c("Right", "Centre/Right")
  # Striker is wide only when a 3+-forward formation makes the "Centre" side a
  # genuine wide forward; a pure Left/Right striker is wide regardless.
  st_left  <- is_left  | (!is.na(side) & side == "Left/Centre"  & !is.na(n_fwd) & n_fwd >= 3)
  st_right <- is_right | (!is.na(side) & side == "Centre/Right" & !is.na(n_fwd) & n_fwd >= 3)
  fcase(
    position == "Goalkeeper", "GK",
    position == "Wing Back",  "WB",
    position == "Defensive Midfielder", "DM",
    position == "Defender" & is_left,  "LB",
    position == "Defender" & is_right, "RB",
    position == "Defender", "CB",
    position == "Midfielder" & is_left,  "LM",
    position == "Midfielder" & is_right, "RM",
    position == "Midfielder", "CM",
    position == "Attacking Midfielder" & am_left,  "LW",
    position == "Attacking Midfielder" & am_right, "RW",
    position == "Attacking Midfielder", "AM",
    position == "Striker" & st_left,  "LW",
    position == "Striker" & st_right, "RW",
    position == "Striker", "ST",
    default = NA_character_
  )
}

# --- chain centroid -> detailed code (multi-feature), per match --------------
# Opta y-axis: 0 = attacker's right, 100 = attacker's left, 50 = centre.
# Bands 30/70 + avg_x full-back gate (CBs sit deep ~37, full-backs ~44).
panna_detail <- function(position, avg_x, avg_y) {
  fcase(
    position == "Goalkeeper", "GK",
    position == "Wing Back",  "WB",
    position == "Defensive Midfielder", "DM",
    position == "Defender" & avg_y > 70 & avg_x > 38, "LB",
    position == "Defender" & avg_y < 30 & avg_x > 38, "RB",
    position == "Defender", "CB",
    position == "Midfielder" & avg_y > 67, "LM",
    position == "Midfielder" & avg_y < 33, "RM",
    position == "Midfielder", "CM",
    position == "Attacking Midfielder" & avg_y > 67, "LW",
    position == "Attacking Midfielder" & avg_y < 33, "RW",
    position == "Attacking Midfielder", "AM",
    position == "Striker" & avg_y > 70 & avg_x > 60, "LW",
    position == "Striker" & avg_y < 30 & avg_x > 60, "RW",
    position == "Striker", "ST",
    default = NA_character_
  )
}

# ============================================================================
# 1. Appearances: coarse opta_position mode + per-appearance Opta detailed
# ============================================================================
ps_path <- file.path(src_dir, "opta_player_stats.parquet")
if (!file.exists(ps_path)) {
  cat("::warning::opta_player_stats.parquet missing — aborting.\n")
  quit(status = 0)
}

ps <- read_parquet(ps_path, col_select = c(
  "player_id", "player_name", "match_id", "competition", "season",
  "position", "position_side", "team_formation", "gameStarted"))
setDT(ps)
ps <- ps[!is.na(player_id) & competition %in% names(COMP_TO_CODE)]
ps[, league := unname(COMP_TO_CODE[competition])]

# Coarse season mode (Substitute demoted) — kept for back-compat + audit.
opta_pos <- ps[, .(
  opta_position = {
    pool <- position[!is.na(position) & position != "Substitute"]
    if (!length(pool)) pool <- position[!is.na(position)]
    mode_chr(pool)
  },
  player_name = player_name[1L]
), by = .(player_id, league, season)]

# Per-appearance Opta detailed over sided STARTS, then mode per player-season.
starts <- ps[gameStarted == 1 & position != "Substitute" & position_side %in% REAL_SIDES]
starts[, det := opta_detail(position, position_side, team_formation)]
opta_det <- starts[!is.na(det), .(
  opta_detailed_position = mode_chr(det),
  n_starts = .N
), by = .(player_id, league, season)]

cat(sprintf("  opta_position: %d rows · opta_detailed (>=1 sided start): %d rows\n",
            nrow(opta_pos), nrow(opta_det)))

# Per-(player, match) coarse position for the panna centroid join below.
match_pos <- ps[gameStarted == 1 & !is.na(position) & position != "Substitute",
                .(position = position[1L]), by = .(player_id, match_id, league)]

# ============================================================================
# 2. Chains: per-match centroid -> per-match panna label -> season mode
#    plus season-level aux aggregates (avg_x/avg_y/sd/n_touches).
# ============================================================================
panna_parts <- list()
aux_parts <- list()

for (comp in names(COMP_TO_CODE)) {
  code <- COMP_TO_CODE[comp]
  cp <- file.path(blog_dir, sprintf("chains-%s.parquet", code))
  if (!file.exists(cp)) {
    cat(sprintf("  skip %-22s (no chains-%s.parquet)\n", comp, code))
    next
  }
  cat(sprintf("  %s: ", code)); flush.console()
  ch <- read_parquet(cp, col_select = c("player_id", "match_id", "season", "x", "y"))
  setDT(ch)
  ch <- ch[!is.na(player_id) & !is.na(x) & !is.na(y)]

  # Season-level aux aggregates (movement profile inputs).
  aux <- ch[, .(
    avg_x = mean(x), avg_y = mean(y),
    sd_x = sd(x), sd_y = sd(y), n_touches = .N
  ), by = .(player_id, season)]
  aux[, league := code]
  aux_parts[[code]] <- aux

  # Per-match centroids, trusted only above the touch floor.
  cent <- ch[, .(avg_x = mean(x), avg_y = mean(y), n_m = .N),
             by = .(player_id, match_id, season)]
  cent <- cent[n_m >= MATCH_TOUCH_MIN]
  # Attach the match's coarse Opta position.
  cent <- merge(cent, match_pos[league == code, .(player_id, match_id, position)],
                by = c("player_id", "match_id"), all.x = TRUE)
  cent <- cent[!is.na(position)]
  cent[, det := panna_detail(position, avg_x, avg_y)]
  pann <- cent[!is.na(det), .(
    panna_detailed_position = mode_chr(det)
  ), by = .(player_id, season)]
  pann[, league := code]
  panna_parts[[code]] <- pann

  cat(sprintf("%d players (%d trusted player-matches)\n",
              nrow(pann), nrow(cent)))
  rm(ch); invisible(gc(verbose = FALSE))
}

if (length(aux_parts) == 0L) {
  cat("::warning::No chains files found — aborting.\n")
  quit(status = 0)
}
aux_agg   <- rbindlist(aux_parts, use.names = TRUE, fill = TRUE)
panna_agg <- rbindlist(panna_parts, use.names = TRUE, fill = TRUE)

# ============================================================================
# 3. Merge all sources + build the hybrid
# ============================================================================
out <- opta_pos
out <- merge(out, opta_det,   by = c("player_id", "league", "season"), all.x = TRUE)
out <- merge(out, panna_agg,  by = c("player_id", "league", "season"), all.x = TRUE)
out <- merge(out, aux_agg,    by = c("player_id", "league", "season"), all.x = TRUE)

# Hybrid: Opta side when we have enough sided starts, else panna centroid.
out[, n_starts := fifelse(is.na(n_starts), 0L, n_starts)]
opta_ok <- !is.na(out$opta_detailed_position) & out$n_starts >= MIN_STARTS
out[, detailed_position := fifelse(opta_ok, opta_detailed_position,
                                   panna_detailed_position)]
out[, pos_source := fcase(
  opta_ok, "opta",
  !is.na(panna_detailed_position), "panna",
  default = NA_character_)]

# Winger promotion (centroid backstop): formation-aware Opta already catches
# the common wide forwards (Saka, Mbeumo). This only rescues the residue — a
# striker Opta reads central (odd/ambiguous formation) whom the INDEPENDENT
# chain centroid clearly places wide. Promote only ST/AM -> winger (never
# demote), and only when the centroid agrees, so a genuine front-two striker
# (panna = ST) stays ST.
promote <- opta_ok &
  out$opta_detailed_position %in% c("ST", "AM") &
  out$panna_detailed_position %in% c("LW", "RW")
out[promote, detailed_position := panna_detailed_position]
out[promote, pos_source := "opta+panna"]
out[, pos_agreement := fifelse(
  !is.na(opta_detailed_position) & !is.na(panna_detailed_position),
  opta_detailed_position == panna_detailed_position, NA)]

# ============================================================================
# 4. Audit
# ============================================================================
cat("\n=== Output summary ===\n")
cat(sprintf("  Total rows: %d\n", nrow(out)))
cat(sprintf("  detailed_position present: %d (%.0f%%)  [opta %d · panna %d]\n",
            sum(!is.na(out$detailed_position)),
            100 * mean(!is.na(out$detailed_position)),
            sum(out$pos_source == "opta", na.rm = TRUE),
            sum(out$pos_source == "panna", na.rm = TRUE)))
both <- out[!is.na(pos_agreement)]
cat(sprintf("  opta vs panna agree: %.0f%% of %d comparable rows\n",
            100 * mean(both$pos_agreement), nrow(both)))
cat("  detailed_position counts:\n")
print(out[!is.na(detailed_position), .N, by = detailed_position][order(-N)])

cat("\n=== Top disagreements (opta vs panna, latest season) ===\n")
.cl <- out[!is.na(pos_agreement) & pos_agreement == FALSE]
.sy <- suppressWarnings(as.integer(substr(.cl$season, 1, 4)))
if (length(.sy) && any(!is.na(.sy))) {
  ls <- .cl$season[which.max(.sy)]
  print(head(out[season == ls & pos_agreement == FALSE,
    .(player_name, league, opta_position,
      opta = opta_detailed_position, panna = panna_detailed_position,
      n_starts, avg_x = round(avg_x, 1), avg_y = round(avg_y, 1))][order(-n_starts)], 15))
}

# Write
setcolorder(out, c("player_id", "player_name", "league", "season",
  "opta_position", "opta_detailed_position", "panna_detailed_position",
  "detailed_position", "pos_source", "pos_agreement", "n_starts",
  "avg_x", "avg_y", "sd_x", "sd_y", "n_touches"))
write_parquet(out, out_path, compression = "snappy")
cat(sprintf("\nWrote %s (%d rows, %.1f KB)\n",
            out_path, nrow(out), file.info(out_path)$size / 1024))
