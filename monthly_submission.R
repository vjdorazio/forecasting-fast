library(arrow)
library(dplyr)
library(tidyr)
library(tools)

# ------------------- Input Args -------------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: monthly_sumbission.R <mode> <draws_dir>")
}
mode <- args[1]              # "baseline" or "tlags"
draws_dir <- args[2]         # e.g. /scratch/forecasting-fast/data/draws/lambda1_xyz

# ------------------- Select 12 Valid Files -------------------
all_files <- list.files(draws_dir, pattern = "\\.parquet$", full.names = TRUE)

if (mode == "baseline") {
  draw_files <- all_files[!grepl("tlags|splags|mov_sum", all_files)]
} else if (mode == "tlags") {
  draw_files <- all_files[grepl("_tlags_", all_files) & !grepl("splags|mov_sum", all_files)]
} else {
  stop("Invalid mode: must be 'baseline' or 'tlags'")
}

if (length(draw_files) != 12) {
  stop(paste("Expected 12 draw files, found", length(draw_files)))
}

cat("Processing", length(draw_files), "draw files for monthly submission...\n")

# ------------------- Load and Process Each File -------------------
monthly_results <- lapply(draw_files, function(f) {
  df <- read_parquet(f)
  df$month_id <- as.integer(df$month_id)
  df$priogrid_gid <- as.integer(as.character(df$priogrid_gid))
  df$draw <- lapply(df$draw, as.integer)
  
  df <- df %>%
    rowwise() %>%
    mutate(
      outcome_n = as.integer(round(mean(draw))),
      outcome_p = round(mean(draw >= 1), 4)
    ) %>%
    ungroup() %>%
    select(priogrid_gid, month_id, outcome_n, outcome_p)
  
  return(df)
})

# ------------------- Combine and Save -------------------
final_monthly <- bind_rows(monthly_results) %>%
  arrange(priogrid_gid, month_id) %>%
  mutate(
    priogrid_gid = as.integer(priogrid_gid),
    month_id = as.integer(month_id),
    outcome_n = as.integer(outcome_n),
    outcome_p = as.numeric(outcome_p)
  )

out_dir <- file.path(draws_dir, "submission")
dir.create(out_dir, showWarnings = FALSE)

out_file <- file.path(out_dir, "submission_monthly.parquet")
write_parquet(final_monthly, out_file)

cat("Saved monthly submission file to:", out_file, "\n")
