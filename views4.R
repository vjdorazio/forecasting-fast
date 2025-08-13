# views4_crps_hpc.R
# Computes CRPS for each grid/month by comparing Tweedie draws to ground truth.
# Produces:
#   (1) Per-row CRPS parquet file
#   (2) Updates a global CSV with the average CRPS for this draw file.
# Logs both the draw file and the ground truth file being used.

rm(list = ls())

library(arrow)
library(scoringRules)
library(dplyr)
library(tools)

# ---------------- Parse Input ----------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript views4.R <draw_file>")
}
draw_file <- args[1]

cat("Processing CRPS for:", draw_file, "\n")

# ---------------- Paths ----------------
hp_tag <- basename(dirname(draw_file))  # e.g., "lambda1_twp1.75_mm20_rt9000"
scratch_root <- file.path(path.expand("~"), "scratch", "forecasting-fast")
forecast_root <- file.path(path.expand("~"), "forecasting-fast")

# Where to save outputs
crps_dir <- file.path(scratch_root, "data", "crps", hp_tag)
dir.create(crps_dir, recursive = TRUE, showWarnings = FALSE)

# Output file paths
draw_basename <- file_path_sans_ext(basename(draw_file))
crps_output <- file.path(crps_dir, paste0(draw_basename, "_crps.parquet"))
summary_csv <- file.path(crps_dir, "crps_summary.csv")

# ---------------- Load Draws and GT ----------------
draws_df <- read_parquet(draw_file)

# Extract the forecast month_id (all rows share the same forecast month)
month_id <- unique(draws_df$month_id)
if (length(month_id) != 1) stop("Expected one unique month_id per draw file.")

# Ground truth file (must match forecast month)
gt_file <- file.path(forecast_root, "data", "gt_snapshots", paste0("gt_", month_id, ".parquet"))
if (!file.exists(gt_file)) stop(paste("Ground truth file not found:", gt_file))

cat("Processing draw file:", draw_file, "\n")
cat("Will use ground truth file:", gt_file, "\n")

gt_df <- read_parquet(gt_file) %>% select(priogrid_gid, month_id, gt)

# ---------------- Ensure Matching Types ----------------
gt_df$priogrid_gid <- as.integer(gt_df$priogrid_gid)
gt_df$month_id <- as.integer(gt_df$month_id)

draws_df$priogrid_gid <- as.integer(as.character(draws_df$priogrid_gid))  # handle factors
draws_df$month_id <- as.integer(draws_df$month_id)

# ---------------- Join by Grid and Month ----------------
eval_df <- inner_join(gt_df, draws_df,
                      by = c("priogrid_gid" = "priogrid_gid",
                             "month_id" = "month_id"))

if (nrow(eval_df) == 0) {
  stop("Join failed: no matching rows between draws and ground truth.")
}

# ---------------- Compute CRPS ----------------
crps_values <- sapply(seq_len(nrow(eval_df)), function(i) {
  y_obs <- eval_df$gt[i]
  y_draws <- eval_df$draw[[i]]  # 1,000 draws
  crps_sample(y = y_obs, dat = y_draws)
})

# ---------------- Save Per-Row CRPS ----------------
crps_df <- data.frame(
  month_id = eval_df$month_id,
  priogrid_gid = eval_df$priogrid_gid,
  crps = crps_values
)

write_parquet(crps_df, crps_output)
cat("Saved CRPS parquet:", crps_output, "\n")

# ---------------- Append Average CRPS to Summary ----------------
avg_crps <- mean(crps_values, na.rm = TRUE)
summary_row <- data.frame(
  draw_file = basename(draw_file),
  gt_file = basename(gt_file),
  avg_crps = round(avg_crps, 4)
)

if (!file.exists(summary_csv)) {
  write.table(summary_row, file = summary_csv, sep = ",", row.names = FALSE, col.names = TRUE, append = FALSE)
} else {
  write.table(summary_row, file = summary_csv, sep = ",", row.names = FALSE, col.names = FALSE, append = TRUE)
}

cat("Appended average CRPS to:", summary_csv, "\n")
cat("Average CRPS for this file:", round(avg_crps, 4), "\n")
cat("Done with:", draw_file, "\n")
