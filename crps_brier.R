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

mydata <- read_parquet("~/scratch/forecasting-fast/data/full_pred/h10_202404_4_tlags_basline_lambda1_twp1.75_mm20_rt9000_full_pred.parquet")

# Keep the most recent 60 months
mydata <- mydata[which(mydata$month_id > (max(mydata$month_id)-60)), ]

# Calculate the 60-month historical probability for each grid using ged_sb
hist_60_df <- mydata %>%
  group_by(priogrid_gid) %>%
  summarize(hist_60_b_prob = mean(ged_sb >= 1, na.rm = TRUE))


hist_60_df$priogrid_gid <- as.integer(as.character(hist_60_df$priogrid_gid))
# Print a sample
head(hist_60_df)



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
# mode <- "tlags"
# 


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
  
  
  
  ##################################
  

  
  print(input_file_name)
  # Load files
  params_df <- read_parquet(params_file)
  pred_df <- read_parquet(input_file)
 
  
  # 2. Prepare and Filter Validation Data
  # ----------------------------------------------------------------
  # Rename columns for consistency and filter for the two validation months
  pred_df$gt <- pred_df$pred_ged_sb
  
  pred_df$predict <- pred_df$predict
  pred_df$month_id <- pred_df$pred_month_id
  val_months <- c(498, 510)
  val_df <- pred_df[pred_df$month_id %in% val_months, ]
  
  
 
  
  
  # 3. Align Data Types and Join
  # ----------------------------------------------------------------
  # Convert all join keys to a consistent integer type to prevent join errors
  val_df$priogrid_gid <- as.integer(as.character(val_df$priogrid_gid))
  params_df$priogrid_gid <- as.integer(params_df$priogrid_gid)
  
  # Join the validation data with parameters
  df <- inner_join(val_df, params_df, by = "priogrid_gid")
  #df <- left_join(df, hist_60_df, by = "priogrid_gid")
 
  
  # 4. Validate the Join
  # ----------------------------------------------------------------
  if (nrow(df) == 0) {
    stop("CRITICAL ERROR: The join resulted in an empty data frame. No matching records were found for the validation months.")
  }
  
  
  # 5. Calculate Main Model Scores
  # ----------------------------------------------------------------
  df <- df %>%
    mutate(
      predict = as.numeric(predict),
      theta = as.numeric(theta),
      power = as.numeric(power),
      gt = as.numeric(gt),
      ged_sb = as.numeric(ged_sb), 
      mu = predict + smidge
    )
  
  # # Use mapply to compute p_event row-by-row
  # df$p_event <- mapply(
  #   function(mu, phi, power) {
  #     1 - ptweedie(q = 1, mu = mu, phi = phi, power = power)
  #   },
  #   df$mu,
  #   df$theta,
  #   df$power
  # )
  
  

  
  # Generate draws and compute the main model's CRPS
  crps_vals <- numeric(nrow(df))
  draw_list <- lapply(seq_len(nrow(df)), function(i) {
    row <- df[i, ]
    mu <- row$predict + smidge
    draws <- round(rtweedie(n = n, power = row$power, mu = mu, phi = row$theta), 0)
    crps_vals[i] <<- crps_sample(y = row$gt, dat = draws)
    
    data.frame(
      month_id = row$month_id,
      priogrid_gid = row$priogrid_gid,
      draw = seq_len(n),
      outcome = draws
    )
  })
  draws_df <- do.call(rbind, draw_list)
  avg_crps <- mean(crps_vals, na.rm = TRUE)
  
  
  ######  brier using draws 
  
  draws_grouped <- group_by(draws_df, month_id, priogrid_gid)
  p_event_df <- summarise(draws_grouped, p_event = mean(outcome >= 1), .groups = "drop")
  
  # Join back into main df (which already has month_id and priogrid_gid)
  df <- left_join(df, p_event_df, by = c("month_id", "priogrid_gid"))
  
  
  df$brier_score <- (df$p_event - as.numeric(df$gt >= 1))^2
  avg_brier <- mean(df$brier_score, na.rm = TRUE)
  
  
  
  # 6. Save Model Draws
  # ----------------------------------------------------------------
  draws_dir <- file.path(scratch_root, "data", "draws", "draw_brier_val", hp_tag)
  dir.create(draws_dir, recursive = TRUE, showWarnings = FALSE)
  draws_file <- file.path(draws_dir, paste0(base_val, "_val_draws.parquet"))
  write_parquet(draws_df, draws_file)
  
  
  # 7. Calculate Benchmark Scores
  # ----------------------------------------------------------------
  # "All Zero" Benchmark
  brier_zero <- mean((0 - as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  crps_zero <- mean(abs(0 - df$gt), na.rm = TRUE)
  
  # "Last Historical" (Probabilistic Persistence) Benchmark using Poisson
  # p_event_hist <- 1 - dpois(x = 0, lambda = df$ged_sb)
  # brier_hist <- mean((p_event_hist - as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  
  
  
  # Compute empirical p_event_hist using rpois draws
  p_event_hist_vec <- sapply(seq_len(nrow(df)), function(i) {
    lambda <- df$ged_sb[i]
    if (is.na(lambda) || lambda < 0) return(NA)
    draws <- rpois(n = n, lambda = lambda)
    mean(draws >= 1)
  })
  
  brier_hist <- mean((p_event_hist_vec - as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  
  
  crps_hist_vals <- sapply(seq_len(nrow(df)), function(i) {
    lambda <- df$ged_sb[i]
    if (is.na(lambda) || lambda < 0) return(NA)
    poisson_draws <- rpois(n = n, lambda = lambda)
    crps_sample(y = df$gt[i], dat = poisson_draws)
  })
  crps_hist <- mean(crps_hist_vals, na.rm = TRUE)
  
  
  #brier_60_hist <- mean((df$hist_60_b_prob -  as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  
  
  # 8. Create and Save Final Summary
  # ----------------------------------------------------------------
  crps_summary <- data.frame(
    forecast_file = base_val,
    month_id = paste(val_months, collapse = "_"),
    avg_brier = round(avg_brier, 8),
    avg_crps = round(avg_crps, 8),
    brier_zero = round(brier_zero, 8),
    crps_zero = round(crps_zero, 8),
    brier_hist = round(brier_hist, 8),
    crps_hist = round(crps_hist, 8)
    #brier_60_hist = round(brier_60_hist,8)
  )
  
  crps_summary_file <- file.path(scratch_root, "data", "score", hp_tag, paste0(mode,"draw_brier_val_score_summary.csv"))
  dir.create(dirname(crps_summary_file), recursive = TRUE, showWarnings = FALSE)
  
  if (!file.exists(crps_summary_file)) {
    write.table(crps_summary, crps_summary_file, sep = ",", row.names = FALSE, col.names = TRUE)
  } else {
    write.table(crps_summary, crps_summary_file, sep = ",", row.names = FALSE, col.names = FALSE, append = TRUE)
  }
  
  
  # 9. Final Console Output
  # ----------------------------------------------------------------
  cat("Validation draws saved to:", draws_file, "\n")
  cat("Validation summary appended to:", crps_summary_file, "\n")
  


  
  
  
} else if (run_flag == "test") {
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
  params_dir <- file.path(scratch_root, "data", "params", hp_tag)
  id_key <- switch(mode,
                   "tlags" = "tlags_basline_",  # Or use more specific substring if needed
                   "specific" = "tlags_tlag1_dr_mod_gs__pred_growseasdummy",
                   stop("Unknown mode!")
  )
  
  # Build pattern: h<num> + id_key + _params.parquet
  pat <- paste0("^h\\d+.*", id_key, ".*_params\\.parquet$")
  
  files <- list.files(params_dir, pattern = pat, full.names = TRUE)
  stopifnot(length(files) > 0)
  params_file <- files[1]
  
  
  
  ##################################
  
  
  
  
  
  gt_file <- file.path(scratch_root, "data", "gt_snapshots", paste0("gt_", month_id, ".parquet"))
  
  # Load files
  params_df <- read_parquet(params_file)
  pred_df <- read_parquet(input_file)
  gt_df <- read_parquet(gt_file) %>%
    dplyr::select(priogrid_gid, month_id, gt)
  
  

  

  
  # Convert all join keys to a consistent integer type FIRST
  pred_df$priogrid_gid <- as.integer(as.character(pred_df$priogrid_gid)) # Factor -> Character -> Integer
  params_df$priogrid_gid <- as.integer(params_df$priogrid_gid)             # Character -> Integer
  gt_df$priogrid_gid <- as.integer(gt_df$priogrid_gid)                     # Already integer, but good in pra
  
  gt_df$month_id <- as.integer(gt_df$month_id)
  pred_df$pred_month_id <- as.integer(pred_df$pred_month_id)
  
  # Join the three dataframes
  df <- pred_df %>%
    inner_join(params_df, by = "priogrid_gid") %>%
    inner_join(gt_df, by = c("priogrid_gid" = "priogrid_gid", "pred_month_id" = "month_id"))
  
  df <- left_join(df, hist_60_df, by = "priogrid_gid")
  
  
  # 2. Validate the Join
  # ----------------------------------------------------------------
  # Stop with a clear error if the join resulted in an empty dataframe
  if (nrow(df) == 0) {
    stop("CRITICAL ERROR: The inner_join resulted in an empty data frame. No matching records were found between the input files.")
  }
  
  
  # 3. Calculate Main Model Scores
  # ----------------------------------------------------------------
  # Prepare columns for calculation
  df <- df %>%
    mutate(
      predict = as.numeric(predict),
      theta = as.numeric(theta),
      power = as.numeric(power),
      gt = as.numeric(gt),
      ged_sb = as.numeric(ged_sb), # Ensure historical column is numeric
      mu = predict + smidge
    )
  
  # # Calculate the probability of an event for the main model
  # df$p_event <- as.numeric(mapply(
  #   function(mu, phi, power) {
  #     1 - ptweedie(q = 1, mu = mu, phi = phi, power = power)
  #   },
  #   df$mu,
  #   df$theta,
  #   df$power
  # ))
  

  
  # Generate draws and compute the main model's CRPS
  crps_vals <- numeric(nrow(df))
  draw_list <- lapply(seq_len(nrow(df)), function(i) {
    row <- df[i, ]
    mu <- row$predict + smidge
    draws <- round(rtweedie(n = n, power = row$power, mu = mu, phi = row$theta), 0)
    crps_vals[i] <<- crps_sample(y = row$gt, dat = draws)
    
    data.frame(
      month_id = row$month_id,
      priogrid_gid = row$priogrid_gid,
      draw = seq_len(n),
      outcome = draws
    )
  })
  draws_df <- do.call(rbind, draw_list)
  avg_crps <- mean(crps_vals, na.rm = TRUE)
  
  ######  brier using draws 
  
  draws_grouped <- group_by(draws_df, month_id, priogrid_gid)
  p_event_df <- summarise(draws_grouped, p_event = mean(outcome >= 1), .groups = "drop")
  
  # Join back into main df (which already has month_id and priogrid_gid)
  df <- left_join(df, p_event_df, by = c("month_id", "priogrid_gid"))
  
  
  df$brier_score <- (df$p_event - as.numeric(df$gt >= 1))^2
  avg_brier <- mean(df$brier_score, na.rm = TRUE)
  
  
  
  
  # 4. Save Model Draws
  # ----------------------------------------------------------------
  draws_dir <- file.path(scratch_root, "data", "draws","_draw_brier_test", hp_tag)
  dir.create(draws_dir, recursive = TRUE, showWarnings = FALSE)
  draws_file <- file.path(draws_dir, paste0(forecast_name, "_test_draws.parquet"))
  write_parquet(draws_df, draws_file)
  
  
  # 5. Calculate Benchmark Scores
  # ----------------------------------------------------------------
  # "All Zero" Benchmark (Predicting 0 every time)
  brier_zero <- mean((0 - as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  crps_zero <- mean(abs(0 - df$gt), na.rm = TRUE)
  
  
  
  
  
  
  # "Last Historical" (Probabilistic Persistence) Benchmark using Poisson
  # A. Calculate Brier Score from derived probability P(event > 0)
  # p_event_hist <- 1 - dpois(x = 0, lambda = df$ged_sb)
  # brier_hist <- mean((p_event_hist - as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  
  # Compute empirical p_event_hist using rpois draws
  p_event_hist_vec <- sapply(seq_len(nrow(df)), function(i) {
    lambda <- df$ged_sb[i]
    if (is.na(lambda) || lambda < 0) return(NA)
    draws <- rpois(n = n, lambda = lambda)
    mean(draws >= 1)
  })
  
  brier_hist <- mean((p_event_hist_vec - as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  
  
  
  
  
  
  #Calculate CRPS using draws from a Poisson distribution
  crps_hist_vals <- sapply(seq_len(nrow(df)), function(i) {
    lambda <- df$ged_sb[i]
    if (is.na(lambda) || lambda < 0) return(NA)
    
    # 'n' should be the same number of draws used for your main model
    poisson_draws <- rpois(n = n, lambda = lambda)
    
    crps_sample(y = df$gt[i], dat = poisson_draws)
  })
  crps_hist <- mean(crps_hist_vals, na.rm = TRUE)
  #brier_60_hist <- mean((df$hist_60_b_prob -  as.numeric(df$gt >= 1))^2, na.rm = TRUE)
  
  # 6. Create and Save Final Summary
  # ----------------------------------------------------------------
  # Create a summary data frame with model and benchmark scores
  crps_summary <- data.frame(
    forecast_file = forecast_name,
    month_id = as.integer(month_id),
    avg_brier = round(avg_brier, 8),
    avg_crps = round(avg_crps, 8),
    brier_zero = round(brier_zero, 8),
    crps_zero = round(crps_zero, 8),
    brier_hist = round(brier_hist, 8),
    crps_hist = round(crps_hist, 8)
    #brier_60_hist = round(brier_60_hist,8)
    
  )
  
  # Define summary file path and save/append the results
  crps_summary_file <- file.path(scratch_root, "data", "score", hp_tag, paste0(mode,"draw_brier_test_score_summary.csv"))
  dir.create(dirname(crps_summary_file), recursive = TRUE, showWarnings = FALSE)
  
  if (!file.exists(crps_summary_file)) {
    write.table(crps_summary, crps_summary_file, sep = ",", row.names = FALSE, col.names = TRUE)
  } else {
    write.table(crps_summary, crps_summary_file, sep = ",", row.names = FALSE, col.names = FALSE, append = TRUE)
  }
  
  
  # 7. Final Console Output
  # ----------------------------------------------------------------
  cat("Draws saved to: ", draws_file, "\n")
  cat("Summary updated: ", crps_summary_file, "\n")
  
  
  
  
} else {
  stop("run_flag must be 'val' or 'test'")
}


