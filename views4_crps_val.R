# views4_val_scores.R
# Computes CRPS and Brier Score for validation predictions
# Inputs:
#   (1) Draws file (1000 draws per grid-month, flat or list-column)
# Inferred:
#   Matching prediction file (ground truth: pred_ged_sb)
# Outputs:
#   (1) Per-row parquet (CRPS + Brier for model + baselines)
#   (2) Appendable summary CSV with averages

rm(list = ls())

library(arrow)
library(scoringRules)
library(dplyr)
library(tools)

# ---------------- Parse Input ----------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript views4_val_scores.R <draw_file>")
}
draw_file <- args[1]

cat("Scoring draws file:", draw_file, "\n")

# ---------------- Paths ----------------
hp_tag <- basename(dirname(draw_file))  # e.g. "lambda1_twp1.75_mm20_rt9000"
scratch_root <- file.path(path.expand("~"), "scratch", "forecasting-fast")

# Inferred prediction file path from the draws basename
draw_basename <- file_path_sans_ext(basename(draw_file))              # "..._draws"
prediction_basename <- sub("_draws$", "", draw_basename)              # remove trailing "_draws"
pred_dir <- file.path(scratch_root, "data", "predictions", hp_tag)
prediction_file <- file.path(pred_dir, paste0(prediction_basename, ".parquet"))

if (!file.exists(prediction_file)) {
  stop(paste0("Could not find prediction file inferred from draws: ", prediction_file))
}

# Output dirs
score_dir <- file.path(scratch_root, "data", "scores_val", hp_tag)
dir.create(score_dir, recursive = TRUE, showWarnings = FALSE)

# Output files
perrow_output <- file.path(score_dir, paste0(draw_basename, "_scores.parquet"))
summary_csv   <- file.path(score_dir, "val_score_summary.csv")

cat("Using prediction file:", prediction_file, "\n")
cat("Scores output dir:", score_dir, "\n")

# ---------------- Load Files ----------------
draws_df_raw <- read_parquet(draw_file)
pred_df <- read_parquet(prediction_file)

# ---------------- Reshape draws to list-column if needed ----------------
has_draw_col <- "draw" %in% names(draws_df_raw)
has_outcome_col <- "outcome" %in% names(draws_df_raw)
is_listcol_draw <- has_draw_col && is.list(draws_df_raw$draw)

if (is_listcol_draw) {
  cat("Detected list-column draw format. Proceeding without reshape.\n")
  draws_df <- draws_df_raw
  draws_df$draw <- lapply(draws_df$draw, as.numeric)
  
} else if (has_draw_col && has_outcome_col) {
  cat("Detected flat draw format. Reshaping to list-column by (month_id, priogrid_gid).\n")
  
  draws_df_raw$priogrid_gid <- as.integer(as.character(draws_df_raw$priogrid_gid))
  draws_df_raw$month_id     <- as.integer(draws_df_raw$month_id)
  
  draws_df <- draws_df_raw %>%
    arrange(priogrid_gid, month_id, draw) %>%
    group_by(priogrid_gid, month_id) %>%
    summarise(draw = list(as.numeric(outcome)), .groups = "drop")
  
} else {
  stop("Unrecognized draw file format: need list-column 'draw' or flat with 'draw' and 'outcome'.")
}

# ---------------- Prepare ground truth from prediction file ----------------
pred_df$priogrid_gid  <- as.integer(as.character(pred_df$priogrid_gid))
pred_df$pred_month_id <- as.integer(pred_df$pred_month_id)
pred_df$pred_ged_sb   <- as.numeric(pred_df$pred_ged_sb)
pred_df$ged_sb        <- as.numeric(pred_df$ged_sb)

gt_df <- pred_df %>%
  select(priogrid_gid, month_id = pred_month_id, gt = pred_ged_sb, ged_sb)

# ---------------- Join draws with ground truth ----------------
eval_df <- inner_join(gt_df, draws_df, by = c("priogrid_gid", "month_id"))
if (nrow(eval_df) == 0) stop("Join failed: no matching rows between draws and prediction file.")

cat("Joined draws with ground truth. Rows:", nrow(eval_df), "\n")

# =======================================================
# Compute per-row scores: Model, All-Zero, Last-Historical
# =======================================================
results_df <- lapply(seq_len(nrow(eval_df)), function(i) {
  y_obs   <- eval_df$gt[i]
  y_draws <- eval_df$draw[[i]]
  
  # --- Model scores ---
  crps_model  <- crps_sample(y = y_obs, dat = y_draws)
  p_hat_model <- mean(y_draws >= 1)
  brier_model <- (p_hat_model - as.integer(y_obs >= 1))^2
  
  # --- All-Zero benchmark ---
  crps_zero <- crps_sample(y = y_obs, dat = rep(0, 1000))
  brier_zero <- (0 - as.integer(y_obs >= 1))^2
  
  # --- Last-Historical benchmark ---
  hist_val <- eval_df$ged_sb[i]
  crps_last_hist <- crps_sample(y = y_obs, dat = rep(hist_val, 1000))
  brier_last_hist <- (as.integer(hist_val >= 1) - as.integer(y_obs >= 1))^2
  
  data.frame(
    month_id = eval_df$month_id[i],
    priogrid_gid = eval_df$priogrid_gid[i],
    gt = y_obs,
    crps_model = crps_model,
    brier_model = brier_model,
    crps_zero = crps_zero,
    brier_zero = brier_zero,
    crps_last = crps_last_hist,
    brier_last = brier_last_hist
  )
})

results_df <- do.call(rbind, results_df)

# Save combined per-row parquet
write_parquet(results_df, perrow_output)
cat("Saved per-row CRPS + Brier (model + baselines):", perrow_output, "\n")

# =======================================================
# Append averages to summary CSV (one row per run)
# =======================================================
summary_row <- data.frame(
  draw_file = basename(draw_file),
  pred_file = basename(prediction_file),
  score_type = "model",
  avg_crps  = mean(results_df$crps_model, na.rm = TRUE),
  avg_brier = mean(results_df$brier_model, na.rm = TRUE),
  crps_zero = mean(results_df$crps_zero, na.rm = TRUE),
  brier_zero = mean(results_df$brier_zero, na.rm = TRUE),
  crps_last = mean(results_df$crps_last, na.rm = TRUE),
  brier_last = mean(results_df$brier_last, na.rm = TRUE)
)

if (!file.exists(summary_csv)) {
  write.table(summary_row, file = summary_csv, sep = ",",
              row.names = FALSE, col.names = TRUE, append = FALSE)
} else {
  write.table(summary_row, file = summary_csv, sep = ",",
              row.names = FALSE, col.names = FALSE, append = TRUE)
}

cat("Appended summary row (model + baselines) to:", summary_csv, "\n")
cat("Done.\n")
