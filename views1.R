# views1.R
# the first r script for forecasting at the grid level

# to get the most recent h2o: https://h2o-release.s3.amazonaws.com/h2o/rel-3.46.0/7/index.html

# The following two commands remove any previously installed H2O packages for R.
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Next, we download packages that H2O depends on.
#pkgs <- c("RCurl","jsonlite")
#for (pkg in pkgs) {
#  if (! (pkg %in% rownames(installed.packages()))) { install.packages(pkg) }
#}

# Now we download, install and initialize the H2O package for R.
#install.packages("h2o", type="source", repos="https://h2o-release.s3.amazonaws.com/h2o/rel-3.46.0/7/R")

rm(list=ls())


id_message <- "tlags_basline_dr_mod"

# for reading data
library(arrow)
library(data.table)
library(curl)
library(readr)
library(Metrics)
library(dplyr)

# load H2O and start up an H2O cluster
library(h2o)



#------- w and b imports 

# library(reticulate)
# use_condaenv("r_clean", required = TRUE)  # use your actual env name
# wandb <- import("wandb")

# api_key <- Sys.getenv("WANDB_API_KEY")
# wandb$login(key = api_key)

port <- as.numeric(Sys.getenv("H2O_REST_PORT", unset = "54321"))
ice_dir <- Sys.getenv("ICE_ROOT", unset = tempfile("h2o_tmp_"))
dir.create(ice_dir, recursive = TRUE, showWarnings = FALSE)

# Safer cluster name
cluster_name <- paste0("grid_", Sys.getenv("SLURM_ARRAY_TASK_ID", unset = "1"))


#h2o.init(max_mem_size = "24G")

# Start H2O with safe ports and log location
h2o.init(
  ip = "127.0.0.1",
  port = port,
  name = cluster_name,
  ice_root = ice_dir,
  max_mem_size = "30G"  # Stay under --mem=32G
)
# for boxcox transformations
library(DescTools)

#setwd("/Users/vjdorazio/Desktop/github/forecasting-fast")
getwd()
setwd("/users/ps00068/forecasting-fast")

#--------- date from 

args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  forecast_date <- as.Date(args[1])  
} else {
  forecast_date <- as.Date("2023-10-01")  # Default if no arg provided
}


# --------------- Date config =-----------------------------

data_start_date    <- as.Date("2010-01-01")   # first month of features fed to the model
data_end_date      <- as.Date("2023-06-01")   # last observed feature month (forecast origin)
#forecast_date      <- as.Date("2023-10-01")   # month we want to predict
validation_months  <- 24                       # size of validation feature window (in months)

# -------------Helper functions ------------------------------------
# NOte: Month‑ID 1 corresponds to 1980‑01‑01.

to_month_id   <- function(date) {
  12 * (as.integer(format(date, "%Y")) - 1980L) + as.integer(format(date, "%m"))
}

from_month_id <- function(mid) {
  as.Date(sprintf("%04d-%02d-01", 1980L + (mid - 1L) %/% 12L, 1L + (mid - 1L) %% 12L))
}

months_between <- function(d_future, d_past) {
  to_month_id(d_future) - to_month_id(d_past)
}

# -----------------------  split parameters form the dates ---------------------------
shift         <- months_between(forecast_date, data_end_date)         
start_mid     <- to_month_id(data_start_date)
end_mid       <- to_month_id(data_end_date)
forecast_mid  <- to_month_id(forecast_date)
horizon_max  <- 12                       # largest forecast horizon 12 monht (shift = 12 → 12-month buffer )
val_end_mid  <- end_mid - horizon_max    #  2022-06 with current dates
val_start_mid<- val_end_mid - (validation_months - 1L)  # 24-month window → 2020-07  


print(end_mid)
# source utils script
source("views_utils.R")

# set to sb
myvars <- "sb"

# set to one of -1, -.5, 0, .5, 1
#lambda <- 1
# #tweesdie_power
# tweedie_power <- 1.75
# # how many months out? takes on value of 1-12sa
# #shift <- 1

# # how many models for autoML?
# mm <- 20

# how long to run h2o in seconds?
h2oruntime <- 9000



lambda <- as.numeric(Sys.getenv("LAMBDA", unset = 1))

raw_lambda <- Sys.getenv("LAMBDA")
if (nchar(raw_lambda) == 0) {
  stop("LAMBDA environment variable is empty!")
}
lambda <- as.numeric(raw_lambda)

if (is.na(lambda)) {
  stop(paste0("LAMBDA environment variable '", raw_lambda, "' could not be converted to numeric (it became NA)."))
}
# Now  original BoxCox code
if (lambda < 0) x[x < 0] <- NA



tweedie_power <- as.numeric(Sys.getenv("TWEEDIE_POWER", unset = 1.75))
mm <- as.numeric(Sys.getenv("MM", unset = 20))


# Define scratch output directory (for HPC)
scratch_root <- file.path(path.expand("~"), "scratch", "forecasting-fast")
# Create a hyperparameter tag for organizing results
hp_tag <- paste0("lambda", lambda,
                 "_twp", tweedie_power,
                 "_mm", mm,
                 "_rt", h2oruntime)


source("views_utils.R")

# read pgm parquet data
mydata <- loadpgm()

if (myvars == "sb") {
  
  # 17 climate and food variables
  keep_extra <- c(
    "count_moder_drought_prev10", "cropprop", "growseasdummy","growseasdummy_2024",
    "spei1_gs_prev10", "spei1_gs_prev10_anom", "spei1_gsm_cv_anom",
    "spei1_gsm_detrend", "spei1gsy_lowermedian_count", "spei_48_detrend",
    "tlag1_dr_mod_gs", "tlag1_dr_moder_gs", "tlag1_dr_sev_gs",
    "tlag1_spei1_gsm", "tlag_12_crop_sum", "tlag_12_harvarea_maincrops",
    "tlag_12_irr_maincrops", "tlag_12_rainf_maincrops"
  )

  #keep_extra <-c("pgd_nlights_calib_mean","tlag1_dr_mod_gs","growseasdummy")
  
  # id and calendar metadata
  keep_meta <- c(
    "name", "gwcode", "isoname", "isoab", "isonum",
    "in_africa", "in_middle_east", "country_id",
    "priogrid_gid", "month_id", "Month", "Year"
  )
  
  # build keep list = all `_sb` cols + metadata + 17 extras
  keeps <- getvars(v = "_sb", df = mydata, a = c(keep_meta, keep_extra))
  
  # safety: only keep columns that actually exist
  keeps <- intersect(keeps, names(mydata))
  
  # report missing requested columns
  requested <- c(keep_extra, keep_meta)
  missing_cols <- setdiff(requested, names(mydata))
  if (length(missing_cols) > 0) {
    message("warning: requested columns missing in mydata: ",
            paste(missing_cols, collapse = ", "))
  }
  
  # subset columns
  df <- mydata[, keeps]
}

df <- builddv(s=shift, df=df)
#df <- builddv_growseasdummy(s=shift, df=df)

df <- df[df$month_id <=end_mid & df$month_id >= start_mid,] #df cutt-off section

print("---total predictor month range---")
range(df$month_id)

print("---prediction month range ---")
range(df$pred_month_id,na.rm = TRUE )

print("--- pred ged sb range ---")
range(df$pred_ged_sb, na.rm = TRUE)
# check the shift with grid 178273 because it has SB values
#temp <- df[which(df$priogrid_gid==178273),c("priogrid_gid", "month_id", "pred_month_id", "ged_sb", "pred_ged_sb")]
#temp <- df[df$priogrid_gid=="178273",c("priogrid_gid", "month_id", "pred_month_id", "ged_sb", "pred_ged_sb")]
#temp <- as.data.frame(temp)

# set factors
df$priogrid_gid <- as.factor(df$priogrid_gid)
df$gwcode <- as.factor(df$gwcode)

# before omitting, partition to save the latest month_id
maxmonth <- max(df$month_id)
forecastdata <- df[which(df$month_id==maxmonth),]
forecastmonth <- maxmonth + shift # + 2 # includes the +2 from views to account for data collection
forecastdata$pred_month_id <- forecastmonth


# forecastdata$pred_month_index <- ((forecastdata$pred_month_id - 1) %% 12) + 1
# template_2024 <- read.csv(file.path("csv", "2024_growseasdummy_template.csv"))
# template_2024 <- template_2024[, c("priogrid_gid", "month_index", "growseasdummy")]
# colnames(template_2024)[3] <- "growseasdummy_2024"
# 
# # Merge so each forecast row gets the right cyclic value
# forecastdata <- merge(
#   forecastdata,
#   template_2024,
#   by.x = c("priogrid_gid", "pred_month_index"),
#   by.y = c("priogrid_gid", "month_index"),
#   all.x = TRUE
# )
# 



cat("Range of month_id in forecastdata:\n")
print(range(forecastdata$month_id))
cat("Range of pred_month_id in forecastdata:\n")
print(range(forecastdata$pred_month_id))

cat("\nFirst few rows of forecastdata:\n")
print(head(forecastdata[, c("priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")]))
print(head(forecastdata))


mid_to_ym <- function(mid) format(from_month_id(mid), "%Y%m")
fcst_ym   <- mid_to_ym(forecastmonth)          # e.g. "202501"
h_label   <- paste0("h", shift)                # "h3" … "h12"
task_id   <- Sys.getenv("SLURM_ARRAY_TASK_ID", unset = "1")
run_label <- paste(h_label, fcst_ym, task_id, id_message, hp_tag, sep = "_")


# # w and b configs init
# wandb$init(
#   project = "grid_forecasting",
#   name = run_label,
#   config = dict(
#     forecast_date = as.character(forecast_date),
#     shift = shift,
#     lambda = lambda,
#     tweedie_power = tweedie_power,
#     mm = mm,
#     h2oruntime = h2oruntime,
#     validation_mode = "default",  # can change later
#     features = "sb_default"       # will change in Phase 2
#   )
# )






# omit NA because shift will create NAs
df <- na.omit(df)

# drop if the DV has negative values (it shouldn't)
df <- df[which(df$pred_ged_sb>=0),]

# Box-Cox transformation
df$y <- BoxCox(df$pred_ged_sb+1, lambda=lambda)


# if lambda is 1, this should be true
all(df$y==df$pred_ged_sb)


traindf <- as.h2o(df[df$month_id < val_start_mid, ])

validdf <- as.h2o(df[df$month_id >= val_start_mid & df$month_id <= val_end_mid, ])


cat("Training Data:\n")
cat("  Range of month_id: ", range(traindf$month_id), "\n")
cat("  Range of pred_month_id: ", range(traindf$pred_month_id), "\n")

cat("Validation Data:\n")
cat("  Range of month_id: ", range(validdf$month_id), "\n")
cat("  Range of pred_month_id: ", range(validdf$pred_month_id), "\n")


mydv <- "y"


if(myvars=="sb") {
  predictors <- getvars(v="_sb", df=df, a=NULL)
  predictors <- colnames(df)[(grepl("^ged_sb$|ged_sb_tlag", colnames(df)))]  #predictors <- colnames(df)[grepl("^ged_sb$|_tlag_|splag", colnames(df))]
  #predictors <- colnames(df)[grepl("^ged_sb$|_tlag_|mov_sum_", colnames(df))]
  #rate_features <- c("rate_ged_sb_12_9", "rate_ged_sb_9_6", "rate_ged_sb_6_3", "rate_ged_sb_3_0")
  #predictors <- c(predictors, rate_features)
  predictors <- union(predictors, intersect(keep_extra[c(11)], names(df)))
  predictors <- predictors[which(predictors != "pred_ged_sb")] # mode readable line for same task  predictors <- setdiff(predictors, "pred_ged_sb")
}


#---------------------- debugging info --------------------------
debug_dir <- file.path(scratch_root, "data", "debug", hp_tag)
dir.create(debug_dir, recursive = TRUE, showWarnings = FALSE)
write.csv(data.frame(predictor_names = predictors),
          file = file.path(debug_dir,  paste0("predictor_", id_message, "_", hp_tag, ".csv")),
  row.names = FALSE             
)




# should be TRUE
h2o.isnumeric(traindf[mydv])

cat("Predictors check: sb=", sum(grepl("_sb", predictors)),
    " extra=", sum(predictors %in% keep_extra),
    " total=", length(predictors), "\n", sep = "")

if (!all(predictors %in% colnames(traindf))) {
  stop(paste("Missing in traindf:", 
             paste(setdiff(predictors, colnames(traindf)), collapse = ", ")))
}

cat("Full predictor list:\n")
for (i in seq_along(predictors)) {
  cat(i, ":", predictors[i], "\n")
}


## autoML training
aml <- h2o.automl(x=predictors, y=mydv, training_frame = traindf, validation_frame = validdf, distribution = list(type = "tweedie", tweedie_power = tweedie_power), exclude_algos = c("DeepLearning"),  max_models = mm, seed=4422, max_runtime_secs = h2oruntime)

u <- aml@leaderboard$model_id

#
# model_dir <- file.path("models", run_label)      
# dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)  

# # note that aml@leader is the model, the filename is the model_id which i believe is unique
# h2o.saveModel(object = aml@leader, path = model_dir, force = TRUE)

model_dir <- file.path(scratch_root, "models", run_label)
dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)
h2o.saveModel(object = aml@leader, path = model_dir, force = TRUE)


print(aml@leaderboard)
leader <- aml@leader
# if needing to go back and load a model
# just the leader is saved, and this loads the leader
#leader <- h2o.loadModel("models/StackedEnsemble_AllModels_1_AutoML_12_20230915_220213")


# Save the full leaderboard for this run, grouped by hp_tag folder
leaderboard_dir <- file.path(scratch_root, "new_results", "leaderboards", hp_tag)
dir.create(leaderboard_dir, recursive = TRUE, showWarnings = FALSE)

lb <- as.data.frame(aml@leaderboard)
lb$run_label <- run_label

arrow::write_parquet(
  lb,
  file.path(leaderboard_dir, paste0(run_label, "_leaderboard.parquet"))
)

validdf <- as.h2o(df[df$month_id %in% c(498, 510), ])

perf_val <- h2o.performance(leader, newdata = validdf)

val_row <- data.frame(
  run_label = run_label,              # full identity string you already use
  h_label   = h_label,                
  model_id  = leader@model_id,
  rmse      = h2o.rmse(perf_val),
  mae       = tryCatch(h2o.mae(perf_val), error = function(e) NA_real_),
  #n         = h2o.nobs(perf_val)
  n         = h2o.nrow(validdf)
)


val_dir  <- file.path(scratch_root, "new_results", "validation_metrics", hp_tag)
dir.create(val_dir, recursive = TRUE, showWarnings = FALSE)

# one CSV per id_message under that folder
val_file <- file.path(val_dir, paste0("validation_", id_message, ".csv"))

write.table(
  val_row,
  file = val_file,
  sep = ",",
  row.names = FALSE,
  col.names = !file.exists(val_file),   # write header if creating new
  append = TRUE
)

cat("\nAppended validation row to: ", val_file, "\n")

preds <- h2o.predict(leader, newdata=validdf) # predictions on the validation data

# needs to be as.h2o
forecastdata <- as.h2o(forecastdata)
forecasts <- h2o.predict(leader, newdata=forecastdata) # forecasts using the latest month of data (454)

# inverse the box-cox transformation to return to counts
# this could be sent to utils
m <- as.matrix(preds)
mf <- as.matrix(forecasts)

# when  lambda is -1, BoxCoxInv will set anything greater than or equal to 1 to NA
# as a default, i'll set them equal to the largest prediction less than 1
if(lambda==-1) {
  val <- m[which(m<1),1]
  val <- max(val)
  m[which(m>=1),1] <- val 
  
  # now for forecasts
  val <- mf[which(mf<1),1]
  val <- max(val)
  mf[which(mf>=1),1] <- val 
}

preds <- BoxCoxInv(m, lambda=lambda) - 1
forecasts <- BoxCoxInv(mf, lambda=lambda) - 1

# write the validation predictions
out <- as.data.frame(preds)
validdf <- validdf[,c("priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")]
temp <- as.data.frame(validdf) 
out <- cbind(out, temp)

# fn <- paste("data/predictions/", run_label, "_", leader@model_id, ".parquet", sep='')
# write_parquet(out, fn)


predictions_dir <- file.path(scratch_root, "data", "new_predictions", hp_tag)
dir.create(predictions_dir, recursive = TRUE, showWarnings = FALSE)
fn <- file.path(predictions_dir, paste0(run_label, "_", leader@model_id, ".parquet"))
write_parquet(out, fn)



# write the forecasts
out <- as.data.frame(forecasts)
forecastdata <- forecastdata[,c("priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")]
temp <- as.data.frame(forecastdata) 
out <- cbind(out, temp)

# fn <- paste("data/forecasts/", leader@model_id, run_label, "_", forecastmonth, ".parquet", sep='')
# write_parquet(out, fn)

forecasts_dir <- file.path(scratch_root, "data", "new_forecasts", hp_tag)
dir.create(forecasts_dir, recursive = TRUE, showWarnings = FALSE)
fn <- file.path(forecasts_dir, paste0(leader@model_id, "_", run_label, "_", forecastmonth, ".parquet"))
write_parquet(out, fn)



# ------------------ Forecast Evaluation ------------------

# Load ground truth from snapshot
gt_file <- file.path("data", "gt_snapshots", paste0("gt_", forecastmonth, ".parquet"))
gt_df <- read_parquet(gt_file)

# Use in-memory forecast output
forecast_df <- out  # this is already saved above

# Ensure types match for join
# forecast_df$priogrid_gid <- as.integer(forecast_df$priogrid_gid)
# gt_df$priogrid_gid <- as.integer(gt_df$priogrid_gid)



# Ensure types are compatible for join
gt_df$priogrid_gid <- as.integer(gt_df$priogrid_gid)
out$priogrid_gid <- as.integer(as.character(out$priogrid_gid))  # if it was factor
out$month_id <- as.integer(out$month_id)
out$pred_month_id <- as.integer(out$pred_month_id)

# 🛠️ Join forecast and ground truth
eval_df <- inner_join(gt_df, out, by = c("priogrid_gid" = "priogrid_gid",
                                         "month_id" = "pred_month_id"))
# Safety check
if (nrow(eval_df) == 0) stop(" Join failed: no matching rows between forecast and ground truth.")

#  Compute metrics
mae_val <- mae(eval_df$gt, eval_df$predict)
rmse_val <- rmse(eval_df$gt, eval_df$predict)
range_vals <- range(eval_df$predict, na.rm = TRUE)

#Optional: clear H2O
h2o.removeAll()

#  Report
cat("\n Forecast Evaluation (month_id =", forecastmonth, ")\n")
cat("MAE         :", round(mae_val, 4), "\n")
cat("RMSE        :", round(rmse_val, 4), "\n")
cat("Forecast Range (min, max): (", round(range_vals[1], 2), ", ", round(range_vals[2], 2), ")\n", sep = "")

results_file <- file.path(scratch_root, "new_results","test",paste0(id_message, "_evaluation_results.csv"))
dir.create(dirname(results_file), recursive = TRUE, showWarnings = FALSE)

results_row <- data.frame(
  month_id = forecastmonth,
  lambda = lambda,
  tweedie_power = tweedie_power,
  mm = mm,
  rmse = round(rmse_val, 4),
  mae = round(mae_val, 4),
  forecast_min = round(range_vals[1], 2),
  forecast_max = round(range_vals[2], 2),
  remark = id_message
)

write.table(results_row, file = results_file, append = TRUE, sep = ",",
            col.names = !file.exists(results_file), row.names = FALSE)
            

# w and b logging 
# wandb$log(dict(
#   mae = round(mae_val, 4),
#   rmse = round(rmse_val, 4),
#   forecast_min = round(range_vals[1], 2),
#   forecast_max = round(range_vals[2], 2)
# ))
# wandb$finish()

# Job summary print
cat("\n  job summary:",
    "\n   run_label  : ", run_label,
    "\n   model_dir  : ", model_dir,
    "\n   forecast   : ", fn,
    "\n   hyperparams: ", hp_tag, "\n")