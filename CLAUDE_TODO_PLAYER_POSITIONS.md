# `player-positions.parquet` v2 — multi-feature position classifier

> **v1 SHIPPED** 2026-05-29 (commit on build-blog-data) — first version is
> live on R2 and consumed by the blog (#257 wired up in inthegame-blog
> b8e0d1b). Initial accuracy is good for clear cases but has known gaps at
> edge cases and a major class of wide attackers misclassified.
>
> This document specifies v2: a small multi-feature rule-based upgrade that
> closes the gaps. Same parquet schema, just better detailed_position values
> plus 2 extra columns the blog (and other consumers) can use for alternative
> classifications.

---

## v1 validation findings (EPL 2025-26, 677 players)

### Distribution

| Position | Count | Notes |
|----------|-------|-------|
| (null) | 214 | substitutes / low-touch — expected |
| AM | **103** | over-inflated — see Issue 1 |
| CB | 61 | |
| ST | 53 | |
| LB | 51 | |
| DM | 48 | |
| RB | 43 | |
| CM | 40 | |
| GK | 39 | |
| LM / RM | 7 / 7 | low — most wide mids are coded AM by Opta |
| LW / RW | 5 / 5 | **too low** — see Issue 1 |
| WB | 1 | rare in 4-back leagues |

### Known-player spot check (19 manually labelled)

**Correct (16):** All true fullbacks (Shaw / Robertson / Kerkez / Spence / Truffert / Trippier / Porro / Castagne), true CBs (Van Dijk / Romero / Tarkowski / Dunk / Magalhães), Gravenberch DM, Saka RW (because Opta tags him as Striker), Haaland ST, Isak ST.

**Misclassified (3 CB/FB boundary):**
- W. Saliba (CB) → got RB (avg_y=32.7, just under the 33 threshold)
- M. Guéhi (CB) → got LB (avg_y=69.2, just over the 67 threshold)
- M. van de Ven (CB) → got LB (avg_y=68.3)

**Wide attackers all collapsed to AM (5):**
- Salah (RW) avg_y=20.2 → AM
- Gakpo (LW) avg_y=73.1 → AM
- Doku (LW) avg_y=78.5 → AM
- Bowen (RW) avg_y=24.1 → AM
- Mbeumo (RW) avg_y=35.6 → AM

**Root cause:** v1 only splits `Striker` and `Midfielder` by avg_y, NOT `Attacking Midfielder`. Modern football tags wide attackers as AM (because they often invert to half-spaces), so they all fall into the same bucket.

---

## Why avg_y alone is too crude

Histogram of EPL Defender avg_y (n=147, ≥200 touches):

```
y15-30: 31  (RBs — clean cluster)
y30-65: 78  (CBs — clean cluster)
y65-72: ~10 (BLUR ZONE — LCBs + inverted-LBs overlap)
y72-85: 26  (LBs)
```

There's a real **shoulder around y65-72** where left-centre-backs and inverted fullbacks both register. Single-threshold bands can't resolve them.

**Good news:** avg_x cleanly separates CBs from fullbacks:

| Detailed pos | Mean avg_x | Median |
|--------------|-----------|--------|
| CB | **36.6** (sit deep) | 36.4 |
| LB | **43.1** (push higher) | 43.6 |
| RB | **44.5** (push highest) | 45.4 |

Adding `avg_x > 38` as a fullback gate would catch Saliba, Guéhi, van de Ven staying as CB.

---

## v2 algorithm — multi-feature rule-based

Keep it transparent. No training, no labels, no model. Just richer features + smarter rules. Easy to iterate visually in R.

### New columns in `player-positions.parquet`

Add 2 columns alongside the existing schema:

| Column | Type | Notes |
|--------|------|-------|
| `sd_y` | float | per-player-season sd of chain touch y |
| `sd_x` | float | per-player-season sd of chain touch x |

(`avg_x`, `avg_y`, `n_touches`, `opta_position`, `detailed_position` already exist.)

These give downstream consumers (the blog, future player-profile features) the raw inputs to compute alternative classifications if v3 ever wants different bucketing without re-deriving from chains.

### Classifier (v2)

```r
classify <- function(opta_pos, avg_x, avg_y, sd_y = NA, n_touches = NA) {
  # Confidence guard — too few touches → don't assign
  if (is.na(n_touches) || n_touches < 100) return(NA_character_)
  if (is.na(opta_pos) || opta_pos %in% c("Substitute", "")) return(NA_character_)

  # Opta already specific — pass through
  if (opta_pos == "Goalkeeper") return("GK")
  if (opta_pos == "Wing Back") return("WB")
  if (opta_pos == "Defensive Midfielder") return("DM")

  # ── Defender split: tighter avg_y bands + avg_x fullback gate ──
  # Bands 30/70 (was 33/67) reduces edge cases like Saliba/Guéhi mislabel.
  # avg_x > 38 ensures we don't reclassify deep-sitting CBs (low avg_x)
  # to fullback just because they shaded one side.
  if (opta_pos == "Defender") {
    is_high <- !is.na(avg_x) && avg_x > 38
    if (avg_y > 70 && is_high) return("LB")
    if (avg_y < 30 && is_high) return("RB")
    return("CB")
  }

  # ── Midfielder split (unchanged from v1) ──
  if (opta_pos == "Midfielder") {
    if (avg_y > 67) return("LM")
    if (avg_y < 33) return("RM")
    return("CM")
  }

  # ── NEW: Attacking Midfielder split ──
  # Opta categorises modern wingers as AM. Same bands as Midfielder.
  # No avg_x guard — AMs are already advanced by definition.
  if (opta_pos == "Attacking Midfielder") {
    if (avg_y > 67) return("LW")
    if (avg_y < 33) return("RW")
    return("AM")
  }

  # ── Striker split: needs avg_x > 60 to avoid mis-labelling withdrawn
  #     forwards (who can be wide but don't push high) as wingers ──
  if (opta_pos == "Striker") {
    is_advanced <- !is.na(avg_x) && avg_x > 60
    if (avg_y > 70 && is_advanced) return("LW")
    if (avg_y < 30 && is_advanced) return("RW")
    return("ST")
  }

  # Fallback for any future Opta value we don't know yet
  return(NA_character_)
}
```

### Why these specific thresholds

- **avg_y 30/70 (was 33/67)**: validates against the histogram — the CB cluster runs 30-70 clearly, the LB cluster starts at 72. The 33/67 v1 thresholds were just inside the CB band.
- **avg_x > 38 for fullbacks**: CB median avg_x is 36.6, fullback median 43-45. The 38 boundary leaves a margin.
- **avg_x > 60 for wide strikers**: ensures we don't reclassify drifting #9s as wingers.
- **n_touches < 100 → NA**: confidence guard. Substitutes / cup-only appearances don't have enough sample for stable bucketing.

### Optional v2.1 improvement (defer if time-constrained)

**Mode of per-match centroids instead of season-wide mean.** For each match the player started, compute (avg_x, avg_y) over their touches in that match; take the mode of those per-match centroids across the season. Robust to occasional out-of-position appearances (e.g. a CB filling in at LB for 2 games).

Trade-off: more complex to compute, marginally better accuracy. Skip unless v2 still has edge-case complaints.

---

## Expected impact

Re-running v2 on EPL 2025-26 should:

- **Salah / Bowen / Mbeumo → RW** (currently AM)
- **Gakpo / Doku → LW** (currently AM)
- **Saliba → CB** (currently RB)
- **Guéhi / van de Ven → CB** (currently LB)
- LM / RM / LW / RW counts roughly triple (from ~5 each to ~15-20)
- AM count drops by ~10 (from 103 to ~93)
- CB count up slightly (~3 boundary cases)
- ~20 EPL players reclassified to more accurate positions per league

Across all 10 domestic leagues × ~16 historic seasons that's ~3000+ improved classifications.

## Blog integration

**No blog-side change needed.** `inthegame-blog/football/player-stats.qmd` reads `detailed_position` column directly from the parquet — same column, just better values. The blog's matchPosFilter (#257) already includes all 13 detailed codes in its pill bar, so when LW gets populated with Salah/Doku/Gakpo etc., clicking LW will start returning them automatically.

The 2 new columns (`sd_y`, `sd_x`) are forward-looking — not required for any current blog feature, but useful if we want to surface "movement profile" indicators on player profile pages later (e.g. "high lateral roaming = drifting winger").

## Not in scope

- Per-team formation detection (3-back vs 4-back) — would help with WB classification but rare enough to defer
- Label-based classifier (XGBoost on FBRef/Transfermarkt position data) — escalate only if rules still have meaningful error
- Mixture-of-Gaussians clustering — clusters might not match football positions; skip
- Detailed positions for cup competitions (UCL/UEL/UECL/WC) — not enough games for stable derivation; pass through domestic-league assignment if available
- L. Diaz / similar name-format misses in v1 — separate investigation (probably opta_lineups → blog-data name normalisation drift)
