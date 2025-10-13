# views3_dub_hpc.R
# reads in: (1) point forecasts and (2) grid-specific parameterizations
# takes 1000 draws for the forecast distribution
# writes the views-formatted output

rm(list=ls())

library(arrow)
library(tweedie)

library(scoringRules)  # For crps_sample

# Data manipulation
library(dplyr)        
library(tools)        

# General utilities
library(stringr)


# source utils scrip just in case
setwd("~/forecasting-fast/")
source("views_utils.R")



n <- 1000 # draws for views

smidge <- .00001

set.seed(4422)


#---------<>----------
# Accept forecast file path as argument (for SLURM job)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript views3.R <val|test> <file>")
}
run_flag <- args[1]       # "val" or "test"
input_file <- args[2]
mode <- args[3]

# run_flag <- "val"       # "val" or "test"
# input_file <- "/users/ps00068/scratch/forecasting-fast/data/re_validation/h10_202404_4_tlags_basline_lambda1_twp1.75_mm20_rt9000_re_validation.parquet"
# 
# mode<-"tlags"



scratch_root <- file.path(path.expand("~"), "scratch", "forecasting-fast")
hp_tag <- "lambda1_twp1.75_mm20_rt9000"





if (run_flag == "val") {
  # 1. Find and Load Files
  # ----------------------------------------------------------------
  # VAL: Use special param name pattern
  input_file_name <- basename(input_file)
  base_val <- sub("_re_validation.parquet$", "", input_file_name)
  #params_basename <- paste0(base_val, "_full_pred_50months_params.parquet")
  #params_file <- file.path(scratch_root, "data", "params24", hp_tag, params_basename)
  
  ############ for 24 month params
  params_dir <- file.path(scratch_root, "data", "params", hp_tag)
  id_key <- switch(mode,
                   "tlags" = "tlags_basline_",  
                   "specific" = "tlags_tlag1_dr_mod_gs__pred_growseasdummy",
                   stop("Unknown mode!")
  )
  
  # Build pattern: h<num> + id_key + _params.parquet
  pat <- paste0("^h\\d+.*", id_key, ".*_params\\.parquet$")
  
  files <- list.files(params_dir, pattern = pat, full.names = TRUE)
  stopifnot(length(files) > 0)
  params_file <- files[1]
  
  print(input_file_name)
  # Load files
  params_df <- read_parquet(params_file)
  pred_df <- read_parquet(input_file)
  
  
  # 2. Prepare and Filter Validation Data
  # ----------------------------------------------------------------
  # Rename columns for consistency and filter for the two validation months

  pred_df$predict <- pred_df$predict
  val_months <- c(498, 510)
  val_df <- pred_df[pred_df$month_id %in% val_months, ]
  
  out <- val_df
  
  
  
  
  # 3. Align Data Types and Join
  # ----------------------------------------------------------------
  # Convert all join keys to a consistent integer type to prevent join errors
  val_df$priogrid_gid <- as.integer(as.character(val_df$priogrid_gid))
  params_df$priogrid_gid <- as.integer(params_df$priogrid_gid)
  
  # Join the validation data with parameters
  df <- inner_join(val_df, params_df, by = "priogrid_gid")
  
  
  
  # 4. Validate the Join
  # ----------------------------------------------------------------
  if (nrow(df) == 0) {
    stop("CRITICAL ERROR: The join resulted in an empty data frame. No matching records were found for the validation months.")
  }
  
  
  # 5. Calculate Main Model Scores
  # ----------------------------------------------------------------
  df <- df %>%
    mutate(
      period =run_flag,
      predict = as.numeric(predict),
      outcome_n = as.integer(round(predict)),
      theta = as.numeric(theta),
      power = as.numeric(power),
      gt = as.numeric(pred_ged_sb),
      ged_sb = as.numeric(ged_sb), # Ensure historical column is numeric
      mu = predict + smidge
    )
  
  
  
  # Generate draws and compute the main model's CRPS
  draw_list <- lapply(seq_len(nrow(df)), function(i) {
    row <- df[i, ]
    mu <- row$predict + smidge
    draws <- round(rtweedie(n = n, power = row$power, mu = mu, phi = row$theta), 0)
    
    data.frame(
      month_id = row$month_id,
      priogrid_gid = row$priogrid_gid,
      draw = seq_len(n),
      outcome = draws
    )
  })
  draws_df <- do.call(rbind, draw_list)
  
  
  ######  brier using draws 
  
  draws_grouped <- group_by(draws_df, month_id, priogrid_gid)
  p_event_df <- summarise(draws_grouped, outcome_p = mean(outcome >= 1), .groups = "drop")
  
  df <- left_join(df, p_event_df, by = c("month_id", "priogrid_gid"))  
  
  
  # CORRECTED: Use mapply to compute p_event row-by-row
#   df$outcome_p <- mapply(
#     function(mu, phi, power) {
#       1 - ptweedie(q = 1, mu = mu, phi = phi, power = power)
#     },
# df$mu,
# df$theta,
# df$power
# )
#   
 
  




}else if (run_flag == "test" | run_flag == "forecast") {
  # TEST: Construct param name from model/run
  month_id <- sub(".*_(\\d+)\\.parquet$", "\\1", basename(input_file))
  forecast_name <- tools::file_path_sans_ext(basename(input_file))
  forecast_base <- sub("_[0-9]+$", "", forecast_name)
  parts <- strsplit(forecast_base, "_h", fixed = TRUE)[[1]]
  model_id <- parts[1]
  run_label <- paste0("h", parts[2])
  #params_basename <- paste0(run_label, "_full_pred_50months_params.parquet")
  #params_file <- file.path(scratch_root, "data", "params24", hp_tag, params_basename)


  ############ for 24 month params
  params_dir <- file.path(scratch_root, "data", "params", "full_pred")
  id_key <- switch(mode,
                   "tlags" = "tlags_basline_",  # Or use more specific substring if needed
                   "specific" = "tlags_basline_dr_mod_2022growseasdummy",
                   stop("Unknown mode!")
  )

  # Build pattern: h<num> + id_key + _params.parquet
  pat <- paste0("^h\\d+.*", id_key, ".*_params\\.parquet$")

  files <- list.files(params_dir, pattern = pat, full.names = TRUE)
  stopifnot(length(files) > 0)
  params_file <- files[1]



  ##################################






  # Load files
  params_df <- read_parquet(params_file)
  pred_df <- read_parquet(input_file)
   
  out <- pred_df





  # Convert all join keys to a consistent integer type FIRST
  pred_df$priogrid_gid <- as.integer(as.character(pred_df$priogrid_gid)) # Factor -> Character -> Integer
  params_df$priogrid_gid <- as.integer(params_df$priogrid_gid)             # Character -> Integer
  pred_df$pred_month_id <- as.integer(pred_df$pred_month_id)

  # Join the three dataframes
  df <- pred_df %>%
    inner_join(params_df, by = "priogrid_gid")



  # 2. Validate the Join
  # ----------------------------------------------------------------
  # Stop with a clear error if the join resulted in an empty dataframe
  if (nrow(df) == 0) {
    stop("CRITICAL ERROR: The inner_join resulted in an empty data frame. No matching records were found between the input files.")
  }


  # 3. Calculate Main Model Scores
  # ----------------------------------------------------------------
  # Prepare columns for calculation
  if (!"pred_ged_sb" %in% names(df)) df$pred_ged_sb <- NA_real_
  df <- df %>%
    mutate(
      period =run_flag,
      predict = as.numeric(predict),
      outcome_n = as.integer(round(predict)),
      theta = as.numeric(theta),
      power = as.numeric(power),
      gt = as.numeric(pred_ged_sb),
      ged_sb = as.numeric(ged_sb), # Ensure historical column is numeric
      mu = predict + smidge
    )

  
  
  
  # Generate draws and compute the main model's CRPS
  draw_list <- lapply(seq_len(nrow(df)), function(i) {
    row <- df[i, ]
    mu <- row$predict + smidge
    draws <- round(rtweedie(n = n, power = row$power, mu = mu, phi = row$theta), 0)
    
    data.frame(
      month_id = row$month_id,
      priogrid_gid = row$priogrid_gid,
      draw = seq_len(n),
      outcome = draws
    )
  })
  draws_df <- do.call(rbind, draw_list)
  
  
  ######  brier using draws 
  
  draws_grouped <- group_by(draws_df, month_id, priogrid_gid)
  p_event_df <- summarise(draws_grouped, outcome_p = mean(outcome >= 1), .groups = "drop")
  
  df <- left_join(df, p_event_df, by = c("month_id", "priogrid_gid"))  
  
  
  
  
  # # Calculate the probability of an event for the main model
  # df$outcome_p <- as.numeric(mapply(
  #   function(mu, phi, power) {
  #     1 - ptweedie(q = 1, mu = mu, phi = phi, power = power)
  #   },
  #   df$mu,
  #   df$theta,
  #   df$power
  # ))
}



out_cols <- c(
  "period",
  "priogrid_gid",   # priofrid_id
  "month_id",       # month id
  "ged_sb",         # ged_sb
  "predict",        # predict
  "outcome_n",      # outcom_n (make sure this column exists)
  "pred_month_id",  # pred month id
  "pred_ged_sb",    # pred ged sb
  "power",          # power
  "theta",          # theta
  "outcome_p"       # outcome_p
)


df_out <- df[, out_cols]

debug_dir <- file.path("~/scratch/forecasting-fast/data/debug_data", mode, run_flag)
if (!dir.exists(debug_dir)) dir.create(debug_dir, recursive = TRUE)
debug_file <- file.path(debug_dir, basename(input_file))
write_parquet(df_out, debug_file)
cat("Wrote debug parquet to: ", debug_file, "\n")
