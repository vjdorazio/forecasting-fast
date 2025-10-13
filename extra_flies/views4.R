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

# ---------------- Load Draws ----------------
draws_df_raw <- read_parquet(draw_file)

# ---------------- Detect and normalize format ----------------
# Expect either:
# 1) list-column format: columns include month_id, priogrid_gid, draw where draw is a list of numeric vectors
# 2) flat format: columns include month_id, priogrid_gid, draw (index), outcome (value per row)

has_draw_col <- "draw" %in% names(draws_df_raw)
has_outcome_col <- "outcome" %in% names(draws_df_raw)
is_listcol_draw <- has_draw_col && is.list(draws_df_raw$draw)

if (is_listcol_draw) {
  cat("Detected list-column draw format. Proceeding without reshape.\n")
  draws_df <- draws_df_raw
  
  # Ensure each list element is numeric
  draws_df$draw <- lapply(draws_df$draw, function(x) as.numeric(x))
  
} else if (has_draw_col && has_outcome_col) {
  cat("Detected flat draw format. Reshaping to list-column by (month_id, priogrid_gid).\n")
  
  # Coerce types for safety
  if ("priogrid_gid" %in% names(draws_df_raw)) {
    draws_df_raw$priogrid_gid <- as.integer(as.character(draws_df_raw$priogrid_gid))
  }
  if ("month_id" %in% names(draws_df_raw)) {
    draws_df_raw$month_id <- as.integer(draws_df_raw$month_id)
  }
  
  # Order by grid, month, then draw index to preserve intended order
  draws_df <- draws_df_raw %>%
    arrange(priogrid_gid, month_id, draw) %>%
    group_by(priogrid_gid, month_id) %>%
    summarise(draw = list(as.numeric(outcome)), .groups = "drop")
  
} else {
  stop("Input draw file is not recognized. Expected either a list-column 'draw' or flat columns 'draw' and 'outcome'.")
}

# ---------------- Extract and validate forecast month ----------------
month_id_vals <- unique(draws_df$month_id)
if (length(month_id_vals) != 1) {
  stop(paste0("Expected one unique month_id per draw file. Found: ", paste(month_id_vals, collapse = ", ")))
}
month_id <- month_id_vals[1]

# ---------------- Load Ground Truth ----------------
gt_file <- file.path(forecast_root, "data", "gt_snapshots", paste0("gt_", month_id, ".parquet"))
if (!file.exists(gt_file)) stop(paste("Ground truth file not found:", gt_file))

cat("Processing draw file:", draw_file, "\n")
cat("Will use ground truth file:", gt_file, "\n")

gt_df <- read_parquet(gt_file) %>% select(priogrid_gid, month_id, gt)

# ---------------- Ensure Matching Types ----------------
gt_df$priogrid_gid <- as.integer(gt_df$priogrid_gid)
gt_df$month_id <- as.integer(gt_df$month_id)

draws_df$priogrid_gid <- as.integer(as.character(draws_df$priogrid_gid))
draws_df$month_id <- as.integer(draws_df$month_id)

# ---------------- Join by Grid and Month ----------------
eval_df <- inner_join(
  gt_df, draws_df,
  by = c("priogrid_gid" = "priogrid_gid", "month_id" = "month_id")
)

if (nrow(eval_df) == 0) {
  stop("Join failed: no matching rows between draws and ground truth.")
}

# ---------------- Validate draw vectors ----------------
# Each draw entry should be a numeric vector with length >= 1
bad_rows <- which(!vapply(eval_df$draw, is.numeric, logical(1)) & !vapply(eval_df$draw, is.double, logical(1)))
if (length(bad_rows) > 0) {
  stop("Found non-numeric draw vectors in the joined data. Check input formatting.")
}

# ---------------- Compute CRPS ----------------
crps_values <- vapply(seq_len(nrow(eval_df)), function(i) {
  y_obs <- eval_df$gt[i]
  y_draws <- eval_df$draw[[i]]
  crps_sample(y = y_obs, dat = y_draws)
}, numeric(1))

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
