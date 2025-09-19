args <- commandArgs(trailingOnly = TRUE)
forecast_file <- args[1]
mode <- args[2]


library(arrow)
forecast_df <- read_parquet(forecast_file)


forecast_file <- "results/model_predictions_454.parquet"
forecast_month <- as.integer(
  regmatches(
    forecast_file,
    regexpr("[0-9]+(?=\\.parquet$)", forecast_file, perl = TRUE)
  )
)


gt_file <- paste0("data/gt_snapshots/gt_", forecast_month, ".parquet")
cat("Ground truth file path:", gt_file, "\n")

gt_df <- read_parquet(gt_file)

eval_df <- inner_join(
  forecast_df, gt_df,
  by = c("priogrid_gid" = "priogrid_gid",
         "pred_month_id" = "month_id")
)

if (nrow(eval_df) == 0) stop("Join failed: no matching rows between forecast and ground truth.")

mae_val <- mae(eval_df$gt, eval_df$predict)
rmse_val <- rmse(eval_df$gt, eval_df$predict)
range_vals <- range(eval_df$predict, na.rm = TRUE)



# Print results
cat("\n Forecast Evaluation (month_id =", forecast_month, ")\n")
cat("MAE         :", round(mae_val, 4), "\n")
cat("RMSE        :", round(rmse_val, 4), "\n")
cat("Forecast Range (min, max): (", round(range_vals[1], 2), ", ", round(range_vals[2], 2), ")\n", sep = "")

# Save results as a row in a CSV (with mode in filename)
results_dir <- "results/test_metrics"
dir.create(results_dir, recursive = TRUE, showWarnings = FALSE)

results_file <- file.path(results_dir, paste0("test_rmse_results_", mode, ".csv"))
results_row <- data.frame(
  forecast_file = basename(forecast_file),
  gt_file = basename(gt_file),
  month_id = forecast_month,
  n = nrow(eval_df),
  rmse = round(rmse_val, 4),
  mae = round(mae_val, 4),
  forecast_min = round(range_vals[1], 2),
  forecast_max = round(range_vals[2], 2),
  mode = mode
)
write.table(
  results_row,
  file = results_file,
  append = TRUE,
  sep = ",",
  col.names = !file.exists(results_file),
  row.names = FALSE
)

cat("Results appended to", results_file, "\n")