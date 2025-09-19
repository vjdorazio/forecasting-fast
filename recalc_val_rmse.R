# args <- commandArgs(trailingOnly = TRUE)
# file <- args[1]
# mode <- args[2]
# 
# library(arrow)
# library(Metrics)
# 
# # Load data
# df <- read_parquet(file)
# 
# # Ensure proper types
# df$predict        <- as.numeric(df$predict)
# df$ged_sb         <- as.numeric(df$ged_sb)         # last historical predictor aligned to prediction rows
# df$pred_ged_sb    <- as.numeric(df$pred_ged_sb)    # ground truth for validation
# df$month_id       <- as.integer(df$month_id)
# df$priogrid_gid   <- as.integer(df$priogrid_gid)
# df$pred_month_id  <- as.integer(df$pred_month_id)
# 
# # Drop rows missing either prediction or ground truth
# df <- df[!is.na(df$predict) & !is.na(df$pred_ged_sb), ]
# 
# # Extract horizon label from filename
# h_label <- regmatches(file, regexpr("h[0-9]{1,2}", file))
# 
# # Overall RMSE - model vs ground truth across all prediction rows
# rmse_all <- rmse(df$pred_ged_sb, df$predict)
# 
# # First prediction month via pred_month_id
# first_prediction_month <- min(df$pred_month_id, na.rm = TRUE)
# df_first <- df[df$month_id == first_prediction_month, ]
# 
# rmse_first_mid <- if (nrow(df_first) > 0) {
#   rmse(df_first$pred_ged_sb, df_first$predict)
# } else {
#   NA_real_
# }
# 
# # Zero baseline RMSE - always predict 0
# rmse_zero <- rmse(df$pred_ged_sb, rep(0, nrow(df)))
# 
# # Last historical baseline RMSE - use ged_sb as the naive prediction
# rmse_last_hist <- rmse(df$pred_ged_sb, df$ged_sb)
# 
# # Output row
# out <- data.frame(
#   file = basename(file),
#   h_label = h_label,
#   rmse_all = round(rmse_all, 4),
#   rmse_first_mid = round(rmse_first_mid, 4),
#   rmse_zero = round(rmse_zero, 4),
#   rmse_last_hist = round(rmse_last_hist, 4),
#   first_mid = first_prediction_month,
#   n_all = nrow(df),
#   n_first = nrow(df_first)
# )
# 
# # Write or append to results CSV
# out_file <- paste0("/users/ps00068/scratch/forecasting-fast/results/val_rmse/all_rmse_results_", mode, ".csv")
# dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
# 
# write.table(
#   out,
#   file = out_file,
#   sep = ",",
#   row.names = FALSE,
#   col.names = !file.exists(out_file),
#   append = TRUE
# )
# 
# cat("Appended results to:", out_file, "\n")



args <- commandArgs(trailingOnly = TRUE)
file <- args[1]
mode <- args[2]

library(arrow)
library(Metrics)

# Load data
df <- read_parquet(file)

# Ensure proper types
df$predict        <- as.numeric(df$predict)
df$ged_sb         <- as.numeric(df$ged_sb)         # last historical predictor aligned to prediction rows
df$pred_ged_sb    <- as.numeric(df$pred_ged_sb)    # ground truth for validation
df$month_id       <- as.integer(df$month_id)
df$priogrid_gid   <- as.integer(df$priogrid_gid)
df$pred_month_id  <- as.integer(df$pred_month_id)

# Drop rows missing either prediction or ground truth
df <- df[!is.na(df$predict) & !is.na(df$pred_ged_sb), ]

# Extract horizon label from filename
h_label <- regmatches(file, regexpr("h[0-9]{1,2}", file))

# RMSE for June 2021 (month_id = 498)
rmse_jun2021 <- if (any(df$month_id == 498)) {
  rmse(df$pred_ged_sb[df$month_id == 498], df$predict[df$month_id == 498])
} else { NA_real_ }

# RMSE for June 2022 (month_id = 510)
rmse_jun2022 <- if (any(df$month_id == 510)) {
  rmse(df$pred_ged_sb[df$month_id == 510], df$predict[df$month_id == 510])
} else { NA_real_ }

# Zero baseline for both months (optional)
rmse_zero_jun2021 <- if (any(df$month_id == 498)) {
  rmse(df$pred_ged_sb[df$month_id == 498], rep(0, sum(df$month_id == 498)))
} else { NA_real_ }
rmse_zero_jun2022 <- if (any(df$month_id == 510)) {
  rmse(df$pred_ged_sb[df$month_id == 510], rep(0, sum(df$month_id == 510)))
} else { NA_real_ }

# Last historical baseline for both months (optional)
rmse_last_hist_jun2021 <- if (any(df$month_id == 498)) {
  rmse(df$pred_ged_sb[df$month_id == 498], df$ged_sb[df$month_id == 498])
} else { NA_real_ }
rmse_last_hist_jun2022 <- if (any(df$month_id == 510)) {
  rmse(df$pred_ged_sb[df$month_id == 510], df$ged_sb[df$month_id == 510])
} else { NA_real_ }

# Output row (add or remove columns as you like)
out <- data.frame(
  file = basename(file),
  h_label = h_label,
  rmse_jun2021 = round(rmse_jun2021, 4),
  rmse_jun2022 = round(rmse_jun2022, 4),
  rmse_zero_jun2021 = round(rmse_zero_jun2021, 4),
  rmse_zero_jun2022 = round(rmse_zero_jun2022, 4),
  rmse_last_hist_jun2021 = round(rmse_last_hist_jun2021, 4),
  rmse_last_hist_jun2022 = round(rmse_last_hist_jun2022, 4),
  n_jun2021 = sum(df$month_id == 498),
  n_jun2022 = sum(df$month_id == 510)
)

# Write or append to results CSV
out_file <- paste0("/users/ps00068/scratch/forecasting-fast/results/val_rmse/new_rmse_results_", mode, ".csv")
dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)

write.table(
  out,
  file = out_file,
  sep = ",",
  row.names = FALSE,
  col.names = !file.exists(out_file),
  append = TRUE
)

cat("RMSE for June 2021 (month_id 498):", round(rmse_jun2021, 4), "\n")
cat("RMSE for June 2022 (month_id 510):", round(rmse_jun2022, 4), "\n")
cat("Appended results to:", out_file, "\n")
