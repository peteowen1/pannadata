# ── Winning-margin sampler ──────────────────────────────────────────────
# Margin (>= 1) of a win, given the match is won by `side` (+1 home, -1 away).
# Goals are independent Poisson(lam_h), Poisson(lam_a); the margin distribution
# is the joint pmf restricted to that side winning, renormalised. The
# win/draw/loss outcome itself still comes from the model's prob_H/D/A.
sample_margin <- function(lam_h, lam_a, side, n) {
  lam_h <- max(lam_h, 0.05); lam_a <- max(lam_a, 0.05)   # guard NA-free degenerate rows
  g <- 0:15
  joint <- outer(dpois(g, lam_h), dpois(g, lam_a))
  d <- outer(g, g, "-") * side            # margin from the winner's view
  w <- tapply(joint[d >= 1], d[d >= 1], sum)
  if (length(w) == 0 || sum(w) <= 0) return(rep(1, n))
  m <- as.integer(names(w))
  if (length(m) == 1L) return(rep(m, n))
  sample(m, n, replace = TRUE, prob = as.numeric(w))
}
