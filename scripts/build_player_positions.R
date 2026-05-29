#!/usr/bin/env Rscript
# Build player-positions.parquet — per-(player_id, league, season) detailed
# position derived from chain x/y averages. Solves the blog#257 ask: Opta's
# raw `position` only emits 8 coarse values (Defender / Midfielder / Striker /
# etc.), no left/right/centre. The blog wants LB/CB/RB/LM/CM/RM/LW/RW pills.
#
# Reads:
#   blog/chains-{CODE}.parquet  (built upstream by build_chains_ci.R; have
#                                player_id + x + y per touch)
#   source/opta_player_stats.parquet  (for opta_position mode per player-season)
#
# Writes:
#   blog/player-positions.parquet  (uploaded to R2 by the existing workflow)
#
# Schema:
#   player_id, player_name, league, season, opta_position,
#   detailed_position, avg_x, avg_y, n_touches

suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(data.table)
})

src_dir <- "source"
blog_dir <- "blog"
out_path <- file.path(blog_dir, "player-positions.parquet")
dir.create(blog_dir, showWarnings = FALSE)

cat("=== Player Positions Builder ===\n")

# Opta competition code -> blog short code (matches build_chains_ci.R output)
COMP_TO_CODE <- c(
  EPL = "ENG", La_Liga = "ESP", Serie_A = "ITA", Bundesliga = "GER",
  Ligue_1 = "FRA", Eredivisie = "NED", Primeira_Liga = "POR",
  Super_Lig = "TUR", Championship = "ENG2",
  Scottish_Premiership = "SCO"
)

# 1. opta_position mode per (player_id, competition, season).
#    "Mode" = most-frequent non-Substitute position across the player's
#    matches in that season. Falls back to most-frequent (incl. Substitute)
#    if every match was a sub appearance.
ps_path <- file.path(src_dir, "opta_player_stats.parquet")
if (!file.exists(ps_path)) {
  cat("::warning::opta_player_stats.parquet missing — cannot derive opta_position mode. Aborting.\n")
  quit(status = 0)
}

# Threshold below which we don't trust the avg_x/avg_y bucket; spec
# suggests 100 touches.
TOUCH_THRESHOLD <- 100L

# Helper: mode of a character vector, NA-safe + "Substitute" demoted
mode_pos <- function(x) {
  x_non_na <- x[!is.na(x)]
  if (length(x_non_na) == 0L) return(NA_character_)
  x_no_sub <- x_non_na[x_non_na != "Substitute"]
  pool <- if (length(x_no_sub) > 0L) x_no_sub else x_non_na
  tab <- table(pool)
  names(tab)[which.max(tab)]
}

ps <- read_parquet(ps_path,
                    col_select = c("player_id", "player_name",
                                    "competition", "season", "position"))
setDT(ps)
ps <- ps[!is.na(player_id) & competition %in% names(COMP_TO_CODE)]
ps[, league := unname(COMP_TO_CODE[competition])]

opta_pos <- ps[, .(
  opta_position = mode_pos(position),
  player_name   = player_name[1L]
), by = .(player_id, league, season)]
cat(sprintf("  opta_position computed for %d (player, league, season) rows\n",
            nrow(opta_pos)))

# 2. Per-player-season x/y aggregates from chains. Process league-by-league
#    to keep memory bounded; the chain files are 100-600 MB each.
chain_aggs <- list()

for (comp in names(COMP_TO_CODE)) {
  code <- COMP_TO_CODE[comp]
  cp <- file.path(blog_dir, sprintf("chains-%s.parquet", code))
  if (!file.exists(cp)) {
    cat(sprintf("  skip %-22s (no chains-%s.parquet)\n", comp, code))
    next
  }
  cat(sprintf("  %s: ", code)); flush.console()
  ch <- read_parquet(cp, col_select = c("player_id", "season", "x", "y"))
  setDT(ch)
  ch <- ch[!is.na(player_id) & !is.na(x) & !is.na(y)]
  agg <- ch[, .(
    avg_x = mean(x, na.rm = TRUE),
    avg_y = mean(y, na.rm = TRUE),
    n_touches = .N
  ), by = .(player_id, season)]
  agg[, league := code]
  chain_aggs[[code]] <- agg
  cat(sprintf("%d players, %d total touches\n", nrow(agg), sum(agg$n_touches)))
  rm(ch); invisible(gc(verbose = FALSE))
}

if (length(chain_aggs) == 0L) {
  cat("::warning::No chains files found in blog/ — cannot derive positions. Aborting.\n")
  quit(status = 0)
}
chains_agg <- rbindlist(chain_aggs, use.names = TRUE, fill = TRUE)
cat(sprintf("  Total chain aggregates: %d (player, league, season) rows\n",
            nrow(chains_agg)))

# 3. Join chain aggregates onto opta_position
out <- merge(opta_pos, chains_agg,
              by = c("player_id", "league", "season"),
              all.x = TRUE)

# 4. classify_detailed
#
# Opta y-axis convention: y=0 -> attacker's right, y=100 -> attacker's left,
# y=50 = centre. Bands at 33/67 per the TODO doc spec.
classify_detailed <- function(opta_pos, avg_y, n_touches) {
  out_pos <- rep(NA_character_, length(opta_pos))
  # Insufficient data -> NA so the blog can decide whether to fall back
  enough <- !is.na(n_touches) & n_touches >= TOUCH_THRESHOLD &
              !is.na(avg_y) & !is.na(opta_pos)
  # Passthrough categories (Opta already distinguishes these)
  passthrough <- c("Goalkeeper" = "GK", "Wing Back" = "WB",
                    "Defensive Midfielder" = "DM",
                    "Attacking Midfielder" = "AM",
                    "Substitute" = NA_character_)
  is_pass <- opta_pos %in% names(passthrough) & !is.na(opta_pos)
  out_pos[is_pass] <- passthrough[opta_pos[is_pass]]

  # Banded categories
  mk_band <- function(left, mid, right) {
    fifelse(avg_y > 67, left, fifelse(avg_y < 33, right, mid))
  }
  is_def <- enough & opta_pos == "Defender"
  out_pos[is_def] <- mk_band("LB", "CB", "RB")[is_def]
  is_mid <- enough & opta_pos == "Midfielder"
  out_pos[is_mid] <- mk_band("LM", "CM", "RM")[is_mid]
  is_str <- enough & opta_pos == "Striker"
  out_pos[is_str] <- mk_band("LW", "ST", "RW")[is_str]
  out_pos
}

out[, detailed_position := classify_detailed(opta_position, avg_y, n_touches)]

# Audit summary
cat("\n=== Output summary ===\n")
cat(sprintf("  Total rows: %d\n", nrow(out)))
cat(sprintf("  With detailed_position: %d (%.0f%%)\n",
            sum(!is.na(out$detailed_position)),
            100 * mean(!is.na(out$detailed_position))))
cat("  detailed_position counts:\n")
print(out[!is.na(detailed_position), .N, by = detailed_position][order(-N)])

# Spot-check a few well-known players
cat("\n=== Spot-check (well-known fullbacks/wingers/CBs) ===\n")
spot <- c("L. Shaw", "T. Alexander-Arnold", "V. van Dijk", "M. Salah",
          "P. Foden", "M. Saka", "K. De Bruyne", "R. Lewandowski",
          "Pedro Porro", "K. Trippier", "A. Robertson")
spot_dt <- out[player_name %in% spot &
                 season == "2025-2026" & league == "ENG"]
if (nrow(spot_dt) > 0L) {
  print(spot_dt[, .(player_name, opta_position, avg_x, avg_y, n_touches,
                     detailed_position)])
} else {
  cat("  (no rows matched — spot-check inconclusive locally)\n")
}

# Write
setcolorder(out, c("player_id", "player_name", "league", "season",
                    "opta_position", "detailed_position",
                    "avg_x", "avg_y", "n_touches"))
write_parquet(out, out_path, compression = "snappy")
cat(sprintf("\nWrote %s (%d rows, %.1f KB)\n",
            out_path, nrow(out),
            file.info(out_path)$size / 1024))
