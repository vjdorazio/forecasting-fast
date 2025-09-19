# views3_pred_draws.R
# reads in: (1) prediction/validation point forecasts and (2) grid-specific parameterizations
# takes 1000 draws for the forecast distribution
# writes the views-formatted flat draw output into draws_val directory

rm(list = ls())

library(arrow)
library(tweedie)

# source utils if needed
# source("views_utils.R")

n <- 1000   # number of draws per row
smidge <- 1e-5
set.seed(4422)

#---------<>----------
# Accept prediction file path as argument (for SLURM job)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript views3_pred_draws.R <prediction_file>")
}
prediction_file <- args[1]

# Identify hp_tag (hyperparameter folder) and scratch root
hp_tag <- basename(dirname(prediction_file))
scratch_root <- file.path(path.expand("~"), "scratch", "forecasting-fast")
#---------<>----------

#---------<>----------
# Build params file path from prediction file name
prediction_name <- tools::file_path_sans_ext(basename(prediction_file))
params_basename <- paste0(prediction_name, "_params.parquet")
params_file <- file.path(scratch_root, "data", "params", hp_tag, params_basename)

# Logging
cat("\n=========================================\n")
cat("Generating draws for prediction/params pair:\n")
cat("  Prediction file :", prediction_file, "\n")
cat("  Params file     :", params_file, "\n")
cat("=========================================\n\n")
#---------<>----------

#---------<>----------
# Load forecast/prediction and params
pred_df <- read_parquet(prediction_file)
params_df <- read_parquet(params_file)



pred_df$priogrid_gid <- as.integer(as.character(pred_df$priogrid_gid))
params_df$priogrid_gid <- as.integer(as.character(params_df$priogrid_gid))

pred_df$pred_month_id <- as.integer(pred_df$pred_month_id)
pred_df$predict <- as.numeric(pred_df$predict)

# Replace non-positive predictions with smidge (to keep mu valid)
pred_df$predict[pred_df$predict <= 0] <- smidge
#---------<>----------

#---------<>----------
# Generate Tweedie draws per prediction row
draw_rows <- list()

for (ii in seq_len(nrow(pred_df))) {
  gid <- pred_df$priogrid_gid[ii]
  mu_val <- pred_df$predict[ii]
  
  # Match params for this grid
  params_row <- params_df[params_df$priogrid_gid == gid, ]
  if (nrow(params_row) != 1) {
    stop(paste("Missing or multiple param rows for priogrid_gid:", gid))
  }
  
  power_val <- params_row$power[1]
  phi_val <- params_row$theta[1]
  
  draws <- round(rtweedie(n = n, mu = mu_val, power = power_val, phi = phi_val), 0)
  
  draw_df <- data.frame(
    month_id = rep(pred_df$pred_month_id[ii], n),
    priogrid_gid = rep(gid, n),
    draw = seq_len(n),
    outcome = draws
  )
  
  draw_rows[[ii]] <- draw_df
}

flat_outdata <- do.call(rbind, draw_rows)
#---------<>----------

#---------<>----------
# Save to scratch/draws_val directory
draws_val_dir <- file.path(scratch_root, "data", "draws_val", hp_tag)
dir.create(draws_val_dir, recursive = TRUE, showWarnings = FALSE)

output_file <- file.path(draws_val_dir, paste0(prediction_name, "_draws.parquet"))
write_parquet(flat_outdata, output_file)

cat("Wrote draws to:", output_file, "\n")
#---------<>----------


