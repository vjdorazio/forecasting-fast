# views1.R - horizon cumulative version with minimal changes

rm(list = ls())

id_message <- ""

library(arrow)
library(data.table)
library(curl)
library(readr)
library(Metrics)
library(dplyr)
library(h2o)

# H2O init
port <- as.numeric(Sys.getenv("H2O_REST_PORT", unset = "54321"))
ice_dir <- Sys.getenv("ICE_ROOT", unset = tempfile("h2o_tmp_"))
dir.create(ice_dir, recursive = TRUE, showWarnings = FALSE)
cluster_name <- paste0("grid_", Sys.getenv("SLURM_ARRAY_TASK_ID", unset = "1"))

h2o.init(
  ip = "127.0.0.1",
  port = port,
  name = cluster_name,
  ice_root = ice_dir,
  max_mem_size = "32G"
)

# working dir
getwd()
setwd("/users/ps00068/forecasting-fast")

# args - keep default Oct 2023
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  forecast_date <- as.Date(args[1])
} else {
  forecast_date <- as.Date("2023-10-01")
}

# Dates and helpers
data_start_date    <- as.Date("2010-01-01")
data_end_date      <- as.Date("2023-06-01")
validation_months  <- 24

to_month_id   <- function(date) 12 * (as.integer(format(date, "%Y")) - 1980L) + as.integer(format(date, "%m"))
from_month_id <- function(mid) as.Date(sprintf("%04d-%02d-01", 1980L + (mid - 1L) %/% 12L, 1L + (mid - 1L) %% 12L))
months_between <- function(d_future, d_past) to_month_id(d_future) - to_month_id(d_past)

# split params
shift         <- months_between(forecast_date, data_end_date)   # stays computed - will be 4 for Oct 2023 vs Jun 2023
start_mid     <- to_month_id(data_start_date)
end_mid       <- to_month_id(data_end_date)
forecast_mid  <- to_month_id(forecast_date)
horizon_max   <- 12
val_end_mid   <- end_mid - horizon_max
val_start_mid <- val_end_mid - (validation_months - 1L)

source("views_utils.R")

myvars <- "sb"
h2oruntime <- 9000

# env
raw_lambda <- Sys.getenv("LAMBDA")
if (nchar(raw_lambda) == 0) stop("LAMBDA environment variable is empty!")
lambda <- as.numeric(raw_lambda)
if (is.na(lambda)) stop("Invalid LAMBDA")

tweedie_power <- as.numeric(Sys.getenv("TWEEDIE_POWER", unset = 1.75))
mm <- as.numeric(Sys.getenv("MM", unset = 20))
horizon <- as.numeric(Sys.getenv("HORIZON", unset = 3))   # 3, 6, or 12

scratch_root <- file.path(path.expand("~"), "scratch", "forecasting-fast")
hp_tag <- paste0("lambda", lambda, "_twp", tweedie_power, "_mm", mm, "_rt", h2oruntime, "_hor", horizon)

# Load data
mydata <- loadpgm()

if (myvars == "sb") {
  keeps <- getvars(
    v = "_sb", df = mydata,
    a = c("name","gwcode","isoname","isoab","isonum","in_africa","in_middle_east",
          "country_id","priogrid_gid","month_id","Month","Year")
  )
  df <- mydata[, keeps]
}

# Feature window
df <- df[df$month_id <= end_mid & df$month_id >= start_mid, ]

# Save a base copy for forecasting rows before label merge
df_base <- df

# Cumulative label builder - starts at t + shift, length = horizon
builddv_cum_dt <- function(df, s, horizon) {
  DT <- as.data.table(df)[, .(priogrid_gid, month_id, ged_sb)]
  setkey(DT, priogrid_gid, month_id)
  target_col <- paste0("pred_horizon_", horizon, "_sb")
  DT[, (target_col) := shift(
    frollsum(ged_sb, n = horizon, align = "left"),
    n = s, type = "lead"
  ),
  by = priogrid_gid]
  DT[, `:=`(
    pred_start_month_id = month_id + s,
    pred_end_month_id   = month_id + s + horizon - 1L
  )]
  DT <- DT[!is.na(get(target_col))]
  DT[, ged_sb := NULL]   # <- drop to avoid suffixes after merge
  DT[]
}

# Build labels and merge back so features remain intact
dt_lab <- builddv_cum_dt(df, s = shift, horizon = horizon)
df <- merge(df, as.data.frame(dt_lab), by = c("priogrid_gid","month_id"), all = FALSE)

cat("---total predictor month range---\n"); print(range(df$month_id))
cat("---pred window start range ---\n");   print(range(df$pred_start_month_id))
cat("---pred window end range   ---\n");   print(range(df$pred_end_month_id))

# factors
df$priogrid_gid <- as.factor(df$priogrid_gid)
df$gwcode <- as.factor(df$gwcode)

# Forecast slice at latest feature month - use base features so it always exists
maxmonth <- max(df_base$month_id)
forecastdata <- df_base[df_base$month_id == maxmonth, ]
forecast_start_mid <- maxmonth + shift
forecast_end_mid   <- forecast_start_mid + horizon - 1L
forecastdata$pred_start_month_id <- forecast_start_mid
forecastdata$pred_end_month_id   <- forecast_end_mid

cat("Range of month_id in forecastdata:\n"); print(range(forecastdata$month_id))
cat("Forecast window:\n"); print(c(forecast_start_mid, forecast_end_mid))

mid_to_ym <- function(mid) format(from_month_id(mid), "%Y%m")
fcst_ym   <- mid_to_ym(forecast_start_mid)
h_label   <- paste0("h", shift)
task_id   <- Sys.getenv("SLURM_ARRAY_TASK_ID", unset = "1")
run_label <- paste(h_label, fcst_ym, task_id, id_message, hp_tag, sep = "_")

# Target and split
df <- na.omit(df)
target_col <- paste0("pred_horizon_", horizon, "_sb")
df$y <- df[[target_col]]

traindf <- as.h2o(df[df$month_id <  val_start_mid, ])
validdf <- as.h2o(df[df$month_id >= val_start_mid & df$month_id <= val_end_mid, ])

cat("Training Data:\n")
cat("  Range of month_id: ", range(traindf$month_id), "\n")
cat("  Range of pred_start_month_id: ", range(traindf$pred_start_month_id), "\n")
cat("Validation Data:\n")
cat("  Range of month_id: ", range(validdf$month_id), "\n")
cat("  Range of pred_start_month_id: ", range(validdf$pred_start_month_id), "\n")

mydv <- "y"

if (myvars == "sb") {
  predictors <- getvars(v = "_sb", df = df, a = NULL)
  # drop only columns that actually exist
  drop_cols <- intersect(
    c(target_col, "pred_start_month_id", "pred_end_month_id"),
    names(df)
  )
  predictors <- setdiff(predictors, drop_cols)
}

# debug predictors
debug_dir <- file.path(scratch_root, "data", "debug", hp_tag)
dir.create(debug_dir, recursive = TRUE, showWarnings = FALSE)
write.csv(data.frame(predictor_names = predictors),
          file = file.path(debug_dir,  paste0("predictor_", id_message, "_", hp_tag, ".csv")),
          row.names = FALSE)

stopifnot(h2o.isnumeric(traindf[[mydv]]))



# AutoML
aml <- h2o.automl(
  x = predictors, y = mydv,
  training_frame = traindf, validation_frame = validdf,
  distribution = list(type = "tweedie", tweedie_power = tweedie_power),
  exclude_algos = c("DeepLearning"),
  max_models = mm, seed = 4422, max_runtime_secs = h2oruntime
)

leader <- aml@leader

# Save model and leaderboard
model_dir <- file.path(scratch_root, "models", run_label)
dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)
h2o.saveModel(object = leader, path = model_dir, force = TRUE)

leaderboard_dir <- file.path(scratch_root, "results", "leaderboards", hp_tag)
dir.create(leaderboard_dir, recursive = TRUE, showWarnings = FALSE)
lb <- as.data.frame(aml@leaderboard); lb$run_label <- run_label
arrow::write_parquet(lb, file.path(leaderboard_dir, paste0(run_label, "_leaderboard.parquet")))

# Validation predictions - keep new fields
preds_val <- as.data.frame(h2o.predict(leader, newdata = validdf))
valid_keep <- c("priogrid_gid","month_id","ged_sb","pred_start_month_id","pred_end_month_id", target_col)
temp_val <- as.data.frame(validdf[, valid_keep])
out_val <- cbind(preds_val, temp_val)

predictions_dir <- file.path(scratch_root, "data", "predictions", hp_tag)
dir.create(predictions_dir, recursive = TRUE, showWarnings = FALSE)
val_file <- file.path(predictions_dir, paste0(run_label, "_", leader@model_id, ".parquet"))
write_parquet(out_val, val_file)

# Forecast predictions at latest month
forecast_h2o <- as.h2o(forecastdata)
preds_fcst <- as.data.frame(h2o.predict(leader, newdata = forecast_h2o))
fc_keep <- c("priogrid_gid","month_id","ged_sb","pred_start_month_id","pred_end_month_id")
temp_fc <- as.data.frame(forecast_h2o[, fc_keep])
out_fc <- cbind(preds_fcst, temp_fc)

forecasts_dir <- file.path(scratch_root, "data", "forecasts", hp_tag)
dir.create(forecasts_dir, recursive = TRUE, showWarnings = FALSE)
fcst_file <- file.path(
  forecasts_dir,
  paste0(leader@model_id, "_", run_label, "_", forecast_start_mid, "_", forecast_end_mid, ".parquet")
)
arrow::write_parquet(out_fc, fcst_file)

# Cumulative evaluation - sum GT across forecast window
aggregate_gt_window <- function(start_mid, end_mid) {
  mids <- seq.int(start_mid, end_mid)
  files <- file.path("data", "gt_snapshots", paste0("gt_", mids, ".parquet"))
  files <- files[file.exists(files)]
  if (length(files) == 0) return(NULL)
  gtl <- lapply(files, read_parquet)
  gt <- rbindlist(gtl, use.names = TRUE, fill = TRUE)  # expect priogrid_gid, month_id, gt
  gt[, .(gt_cum = sum(gt, na.rm = TRUE)), by = .(priogrid_gid)]
}

gt_agg <- aggregate_gt_window(forecast_start_mid, forecast_end_mid)

if (!is.null(gt_agg)) {
  pj <- data.table::as.data.table(out_fc)
  pj[, priogrid_gid := as.integer(as.character(priogrid_gid))]
  eval_df <- merge(gt_agg, pj, by = "priogrid_gid", all = FALSE)
  if (nrow(eval_df) > 0) {
    mae_val <- mae(eval_df$gt_cum, eval_df$predict)
    rmse_val <- rmse(eval_df$gt_cum, eval_df$predict)
    range_vals <- range(eval_df$predict, na.rm = TRUE)
    h2o.removeAll()
    
    cat("\n Forecast Evaluation window (", forecast_start_mid, "-", forecast_end_mid, ")\n", sep = "")
    cat("MAE  :", round(mae_val, 4), "\n")
    cat("RMSE :", round(rmse_val, 4), "\n")
    cat("Pred Range (min, max): (", round(range_vals[1], 2), ", ", round(range_vals[2], 2), ")\n", sep = "")
    
    results_file <- file.path(scratch_root, "results", "evaluation_results_cum.csv")
    dir.create(dirname(results_file), recursive = TRUE, showWarnings = FALSE)
    results_row <- data.frame(
      horizon = horizon,
      start_mid = forecast_start_mid,
      end_mid = forecast_end_mid,
      lambda = lambda,
      tweedie_power = tweedie_power,
      mm = mm,
      rmse = round(rmse_val, 4),
      mae = round(mae_val, 4),
      pred_min = round(range_vals[1], 2),
      pred_max = round(range_vals[2], 2),
      remark = id_message
    )
    write.table(results_row, file = results_file, append = TRUE, sep = ",",
                col.names = !file.exists(results_file), row.names = FALSE)
  } else {
    message("No overlap between forecast preds and GT. Skipping eval.")
  }
} else {
  message("GT files for forecast window not found. Skipping eval.")
}

cat("\n  job summary:",
    "\n   horizon    : ", horizon,
    "\n   forecast   : ", fcst_file,
    "\n   model_dir  : ", model_dir,
    "\n   hyperparams: ", hp_tag, "\n")
