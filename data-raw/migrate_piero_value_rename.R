# migrate_piero_value_rename.R
# One-time migration (2026-07-07): rename panna_value / panna_value_p90 ->
# piero_value / piero_value_p90 in every game_logs*.parquet on the pannadata
# blog-latest release. Companion to the panna-side producer rename — the old
# name implied "per-match panna", which the metric never was (it's the
# EPV+PSV production blend, i.e. Piero's value twin).
#
# Safe to re-run: files already renamed are skipped. build_blog_data.R carries
# a transition shim accepting either name, so ordering vs the panna deploy
# doesn't matter.
#
# Run from the pannadata repo root: Rscript data-raw/migrate_piero_value_rename.R

library(arrow)

repo <- "peteowen1/pannadata"
tag <- "blog-latest"

assets <- piggyback::pb_list(repo = repo, tag = tag)
targets <- grep("^game_logs.*\\.parquet$", assets$file_name, value = TRUE)
cat(sprintf("game_logs assets on %s: %d\n", tag, length(targets)))

tmp <- file.path(tempdir(), "piero_rename")
dir.create(tmp, showWarnings = FALSE, recursive = TRUE)

renames <- c(panna_value = "piero_value", panna_value_p90 = "piero_value_p90")
n_migrated <- 0L
n_skipped <- 0L
failures <- character(0)

for (f in sort(targets)) {
  ok <- tryCatch({
    piggyback::pb_download(f, repo = repo, tag = tag, dest = tmp)
    p <- file.path(tmp, f)
    d <- arrow::read_parquet(p)
    hit <- intersect(names(renames), names(d))
    if (length(hit) == 0) {
      cat(sprintf("  %-32s already migrated (skip)\n", f))
      n_skipped <<- n_skipped + 1L
      file.remove(p)
    } else {
      names(d)[match(hit, names(d))] <- renames[hit]
      # NEVER write back to the downloaded path: arrow memory-maps it during
      # read and Windows then refuses the overwrite (error 1224, the same trap
      # 10b works around). Write a sibling file in a per-asset subdir whose
      # basename matches the asset name (pb_upload uses basename as the asset
      # name), and upload that.
      newdir <- file.path(tmp, "renamed")
      dir.create(newdir, showWarnings = FALSE)
      p_new <- file.path(newdir, f)
      arrow::write_parquet(d, p_new)
      piggyback::pb_upload(p_new, repo = repo, tag = tag, overwrite = TRUE)
      cat(sprintf("  %-32s renamed %s (%s rows)\n", f,
                  paste(hit, collapse = "+"), format(nrow(d), big.mark = ",")))
      n_migrated <<- n_migrated + 1L
      rm(d); gc(verbose = FALSE)
      file.remove(p_new)
      suppressWarnings(file.remove(p))
    }
    TRUE
  }, error = function(e) {
    cat(sprintf("  %-32s FAILED: %s\n", f, conditionMessage(e)))
    failures <<- c(failures, f)
    FALSE
  })
}

cat(sprintf("\nDone: %d migrated, %d already-clean, %d failed\n",
            n_migrated, n_skipped, length(failures)))
if (length(failures) > 0) {
  cat("Failed:", paste(failures, collapse = ", "), "\n")
  quit(status = 1)
}
