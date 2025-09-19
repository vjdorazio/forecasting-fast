#!/usr/bin/env Rscript
# monthly_forecast_submission.R
# Build monthly submission by pairing draws with forecasts monthwise
# Join only on priogrid_gid. month_id is taken from the filename.

## To run this code do the following (as example) :
# 
# # 1. Allocate resources on SLURM (interactive session)
# salloc --job-name=Submission creation --nodes=1 --ntasks=1 --cpus-per-task=2 --mem=48G --time=4:00:00
# 
# # 2. Initialize conda + activate your R environment
# source /shared/software/conda/conda_init.sh && source ~/.bashrc && conda activate /scratch/ps00068/r_clean
# 
# # 3. Move into your project directory (optional, if script is there)
# cd ~/scratch/forecasting-fast
# 
# # 4. Run the R script with mode and draws directory
# Rscript monthly_forecast_submission.R tlags ~/scratch/forecasting-fast/data/draws/lambda1_twp1.75_mm20_rt9000
# 


suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(purrr)
})

# ---------------- args ----------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: monthly_forecast_submission.R <specific|tlags> <draws_dir>")
}
mode      <- args[1]
draws_dir <- normalizePath(args[2], mustWork = TRUE)
if (!(mode %in% c("specific", "tlags"))) stop("Invalid mode. Use 'specific' or 'tlags'.")

# ---------------- paths ----------------
data_root    <- dirname(dirname(draws_dir))
hp_tag       <- basename(draws_dir)
forecast_dir <- file.path(data_root, "forecasts", hp_tag)  # plural per your tree

if (!dir.exists(forecast_dir)) stop("Forecast directory not found: ", forecast_dir)

cat("draws_dir     :", draws_dir, "\n")
cat("forecast_dir  :", forecast_dir, "\n")
cat("hp_tag        :", hp_tag, "\n")
cat("mode          :", mode, "\n\n")

# ---------------- file pickers ----------------
list_mode_files <- function(dir_path, mode) {
  all_files <- list.files(dir_path, pattern = "\\.parquet$", full.names = TRUE)
  fn <- basename(all_files)
  
  if (mode == "tlags") {
    # Match files like *_tlags_lambda1_*.parquet
    keep <- grepl("_tlags_lambda1_.*\\.parquet$", fn)
  } else if (mode == "specific") {
    # Exact ID_KEY logic match
    id_key <- "tlags_tlag1_dr_mod_gs__pred_growseasdummy"
    keep <- grepl(paste0(id_key, ".*\\.parquet$"), fn)
  } else {
    stop(paste("invalid mode:", mode))
  }
  
  return(all_files[keep])
}


draw_files <- list_mode_files(draws_dir,    mode)
fcst_files <- list_mode_files(forecast_dir, mode)

cat("Found draw files    :", length(draw_files), "\n")
cat("Found forecast files:", length(fcst_files), "\n")
if (length(draw_files) != 12) stop("Expected 12 draw files, found ", length(draw_files))
if (length(fcst_files)  != 12) stop("Expected 12 forecast files, found ", length(fcst_files))

# ---------------- month id from filename tail ----------------
# Handles both ..._###_draws.parquet (draws) and ..._###.parquet (forecasts)
get_month_id <- function(path) {
  fn <- basename(path)
  if (!grepl("_(\\d{3})(?:_draws)?\\.parquet$", fn, perl = TRUE)) {
    stop("Cannot parse month_id from filename: ", fn)
  }
  as.integer(sub(".*_(\\d{3})(?:_draws)?\\.parquet$", "\\1", fn, perl = TRUE))
}

draw_months <- vapply(draw_files, get_month_id, integer(1))
fcst_months <- vapply(fcst_files, get_month_id, integer(1))

cat("draw_months  :", paste(draw_months, collapse = ", "), "\n")
cat("fcst_months  :", paste(fcst_months, collapse = ", "), "\n\n")

if (!setequal(draw_months, fcst_months)) {
  stop("Month mismatch between draws and forecasts.\n",
       "draw months: ", paste(sort(unique(draw_months)), collapse = ", "), "\n",
       "fcst months: ", paste(sort(unique(fcst_months)), collapse = ", "))
}

# Reorder forecasts to match draws order by month
fcst_files <- fcst_files[ match(draw_months, fcst_months) ]

# Paired file table
paired <- tibble(
  month_id  = draw_months,
  file_draw = draw_files,
  file_fcst = fcst_files
) %>% arrange(month_id)
print(paired)

# ---------------- helpers ----------------
compute_outcome_p <- function(draw_file) {
  df <- read_parquet(draw_file)
  
  if (!all(c("priogrid_gid", "outcome") %in% names(df))) {
    stop("Draw file missing required columns: ", draw_file)
  }
  
  df %>%
    mutate(priogrid_gid = as.integer(as.character(priogrid_gid))) %>%
    group_by(priogrid_gid) %>%
    summarise(outcome_p = mean(outcome >= 1), .groups = "drop") %>%
    mutate(outcome_p = round(outcome_p, 4))
}


read_predict <- function(fcst_file) {
  df <- read_parquet(fcst_file)
  if (!all(c("priogrid_gid", "predict") %in% names(df))) {
    stop("Forecast file missing required columns: ", fcst_file)
  }
  df %>%
    mutate(priogrid_gid = as.integer(as.character(priogrid_gid))) %>%
    select(priogrid_gid, predict)
}

# ---------------- per month build ----------------
monthly_list <- pmap(
  list(paired$file_draw, paired$file_fcst, paired$month_id),
  function(dfpath, fcpath, mid) {
    cat("\nProcessing month_id:", mid, "\n")
    
    dp <- compute_outcome_p(dfpath)  # priogrid_gid, outcome_p
    dn <- read_predict(fcpath)       # priogrid_gid, predict
    
    # expected 13110 rows before join
    cat("  draws rows:", nrow(dp), " grids:", n_distinct(dp$priogrid_gid), "\n")
    cat("  fcsts rows:", nrow(dn), " grids:", n_distinct(dn$priogrid_gid), "\n")
    
    # join by grid only, month is already paired by filename
    out <- inner_join(dn, dp, by = "priogrid_gid") %>%
      mutate(month_id = as.integer(mid)) %>%
      transmute(priogrid_gid, month_id, outcome_n = as.integer(round(predict)), outcome_p)
    
    cat("  joined rows:", nrow(out), " grids:", n_distinct(out$priogrid_gid), "\n")
    if (nrow(out) != 13110L || n_distinct(out$priogrid_gid) != 13110L) {
      warning("Month ", mid, " has ", nrow(out), " rows and ",
              n_distinct(out$priogrid_gid), " unique grids. Expected 13110.")
    }
    out
  }
)

monthly <- bind_rows(monthly_list) %>%
  arrange(priogrid_gid, month_id)

# ---------------- global diagnostics ----------------
cat("\n========== GLOBAL DIAGNOSTICS ==========\n")
months_present <- sort(unique(monthly$month_id))
cat("Months present           :", paste(months_present, collapse = ", "), "\n")
cat("Rows total               :", nrow(monthly), "\n")
cat("Unique grids overall     :", n_distinct(monthly$priogrid_gid), "\n")

expected_rows <- 13110L * length(months_present)
cat("Expected rows            :", expected_rows, "\n")
if (nrow(monthly) != expected_rows) {
  warning("Row count mismatch. Got ", nrow(monthly), " but expected ", expected_rows)
}
stopifnot(nrow(monthly) > 0)

# ---------------- save and verify ----------------
out_dir <- file.path(draws_dir, "submission")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

full_path   <- file.path(out_dir, paste0("submission_monthly_full_", mode, ".parquet"))
subset_path <- file.path(out_dir, paste0("submission_monthly_every3_", mode, ".parquet"))

write_parquet(monthly, full_path)

subset_3 <- monthly %>%
  filter(month_id %in% c(528L, 531L, 537L)) %>%
  mutate(
    time_id = recode(month_id,
                     `528` = 3L,
                     `531` = 6L,
                     `537` = 12L)
  )
write_parquet(subset_3, subset_path)

cat("\nSaved files:\n  ", full_path, "\n  ", subset_path, "\n")

# Reopen to confirm non empty
check_full   <- read_parquet(full_path)
check_subset <- read_parquet(subset_path)
cat("\n========== SAVE VERIFICATION ==========\n")
cat("Full file rows  :", nrow(check_full),   " cols:", ncol(check_full), "\n")
cat("Every3 file rows:", nrow(check_subset), " cols:", ncol(check_subset), "\n")
cat("Done.\n")
