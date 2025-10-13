rm(list=ls())
library(arrow)
library(h2o)
library(MASS) # using this for rnegbin

# for reading data
library(arrow)
library(dplyr)
library(h2o)





mydata <- read_parquet("~/forecasting-fast/data/2025_pgm/updated_pgm_2025_01-07.parquet")

# Keep the forecast input month
mydata <- mydata[(mydata$month_id == 546),]

cat("month id:", length(mydata$month_id))

port <- as.numeric(Sys.getenv("H2O_REST_PORT", unset = "54321"))
ice_dir <- Sys.getenv("ICE_ROOT", unset = tempfile("h2o_tmp_"))
dir.create(ice_dir, recursive = TRUE, showWarnings = FALSE)

# Safer cluster name
cluster_name <- paste0("grid_", Sys.getenv("SLURM_ARRAY_TASK_ID", unset = "1"))
 # h2o.init(max_mem_size = "32G" )
#Start H2O with safe ports and log location
h2o.init(
  ip = "127.0.0.1",
  port = port,
  name = cluster_name,
  ice_root = ice_dir,
  max_mem_size = "24G"  # Stay under --mem=32G
)
# need to re_run the preduction by loading the models and generate the


args <- commandArgs(trailingOnly = TRUE)
model_folder <- args[1]





#model_folder <- path.expand("~/scratch/forecasting-fast/models/h10_202404_7_tlags_basline_dr_mod_2022growseasdummy_lambda1_twp1.75_mm20_rt9000")
save_dir <-path.expand("~/scratch/forecasting-fast/data/2025_forecast")
folder_name <- basename(model_folder)
shift <- as.integer(sub("h([0-9]+)_.*", "\\1", folder_name))
cat("the shift value is :", shift, "\n")


myvars <- "sb"



setwd("~/forecasting-fast/")
source("views_utils.R")


# ---- Prepare Data ----
if (myvars == "sb") {
  keep_extra <- c(
    "count_moder_drought_prev10", "cropprop", "growseasdummy","growseasdummy_2024",
    "spei1_gs_prev10", "spei1_gs_prev10_anom", "spei1_gsm_cv_anom",
    "spei1_gsm_detrend", "spei1gsy_lowermedian_count", "spei_48_detrend",
    "tlag1_dr_mod_gs", "tlag1_dr_moder_gs", "tlag1_dr_sev_gs",
    "tlag1_spei1_gsm", "tlag_12_crop_sum", "tlag_12_harvarea_maincrops",
    "tlag_12_irr_maincrops", "tlag_12_rainf_maincrops"
  )
  keep_meta <- c(
    "name", "gwcode", "isoname", "isoab", "isonum",
    "in_africa", "in_middle_east", "country_id",
    "priogrid_gid", "month_id", "Month", "Year"
  )
  keeps <- getvars(v = "_sb", df = mydata, a = c(keep_meta, keep_extra))
  keeps <- intersect(keeps, names(mydata))
  requested <- c(keep_extra, keep_meta)
  missing_cols <- setdiff(requested, names(mydata))
  if (length(missing_cols) > 0) {
    message("warning: requested columns missing in mydata: ",
            paste(missing_cols, collapse = ", "))
  }
  df <- mydata[, keeps]
}



#df <- builddv1(s = shift, df = df)
#df <-builddv_growseasdummy(s = shift, df = df)

df$priogrid_gid <- as.factor(df$priogrid_gid)
#df$gwcode <- as.factor(df$gwcode)

df$pred_month_id <- df$month_id + shift

# df1 <- loadpgm()
# df1 <-builddv_growseasdummy(s = shift, df = df1)

df$pred_month_index <- ((df$pred_month_id - 1) %% 12) + 1
template_2024 <- read.csv(file.path("csv", "2024_growseasdummy_template.csv"))
template_2024 <- template_2024[, c("priogrid_gid", "month_index", "growseasdummy")]
colnames(template_2024)[3] <- "growseasdummy_2024"

# Merge so each forecast row gets the right cyclic value
df <- merge(
  df,
  template_2024,
  by.x = c("priogrid_gid", "pred_month_index"),
  by.y = c("priogrid_gid", "month_index"),
  all.x = TRUE
)



# ---- Predictor Columns ----
if (myvars == "sb") {
  predictors <- colnames(df)[(grepl("^ged_sb$|ged_sb_tlag", colnames(df)))]
  predictors <- union(predictors, intersect(keep_extra[c(4,11)], names(df)))
  predictors <- predictors[which(predictors != "pred_ged_sb")]
}

cat("Predictors list (used as features in model):\n")
print(predictors)

cat("\nRange of month_id (date range) in df being used:\n")
print(range(df$month_id, na.rm = TRUE))

cat("\nNumber of rows in df (training + validation):\n")
print(nrow(df))

cat("\nFirst few rows of df (to inspect columns and values):\n")
print(head(df, 10))

# ---- Prepare H2O Frame ----
loaddf <- as.h2o(df)


# ---- Load Model ----
model_candidates <- list.files(model_folder, full.names = TRUE)

print("Model candidates found in folder:")
print(model_candidates)

if (length(model_candidates) < 1) {
  stop(paste("No model found in model folder:", model_folder))
}

model_path <- model_candidates[1]

cat("Loading model from:", model_path, "\n")
model <- h2o.loadModel(model_path)
cat("Model loaded with model name:", model@model_id, "\n")



# ---- Predict on All Data ----
pred_h2o <- h2o.predict(model, loaddf[, predictors])
pred_col <- as.vector(as.data.frame(pred_h2o)[, 1])
pred_df <- as.data.frame(df)
pred_df$predict <- pred_col
################# need to do rounding operation#################

pred_df$predict_round <- round(pred_df$predict)



dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

forecast_dir <- file.path(save_dir, "forecast")


dir.create(forecast_dir, recursive = TRUE, showWarnings = FALSE)



# ---- Save Full-Range Predictions (Only Specified Columns) ----
cols_to_save <- c("predict","priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")
cols_to_save <- cols_to_save[cols_to_save %in% names(pred_df)]  # keep only existing columns

saved_df <- pred_df[, cols_to_save]

dir.create(forecast_dir, recursive = TRUE, showWarnings = FALSE)
full_pred_file <- file.path(forecast_dir, paste0(folder_name,"_",(shift+546), ".parquet"))
write_parquet(pred_df[, cols_to_save], full_pred_file)
cat("Saved  predictions to:", full_pred_file, "\n")

