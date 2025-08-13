
library(arrow)
library(dplyr)
library(tidyr)
library(tools)

# ------------------- Input Args -------------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript generate_submission_pg.R <mode> <draws_dir>")
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

cat("Processing", length(draw_files), "draw files...\n")

# ------------------- Load and Combine All Draw Files -------------------
draw_dfs <- lapply(draw_files, function(f) {
  df <- read_parquet(f)
  df$month_id <- as.integer(df$month_id)
  df$priogrid_gid <- as.integer(as.character(df$priogrid_gid))
  df$draw <- lapply(df$draw, as.integer)  # ensure each draw is a list of ints
  return(df)
})

combined <- bind_rows(draw_dfs)

# ------------------- Expand Draws -------------------
unpacked <- combined %>%
  mutate(draw_id = row_number()) %>%
  unnest_longer(draw, values_to = "value") %>%
  group_by(priogrid_gid, draw_id) %>%
  mutate(draw_index = row_number()) %>%
  ungroup()

# ------------------- Horizon Summarizer -------------------
summarise_horizon <- function(data, horizon_months, time_id) {
  data %>%
    filter(month_id %in% horizon_months) %>%
    group_by(priogrid_gid, draw_index) %>%
    summarise(draw_sum = sum(value), .groups = "drop") %>%
    group_by(priogrid_gid) %>%
    summarise(
      outcome_n = as.integer(round(mean(draw_sum))),
      outcome_p = round(mean(draw_sum >= 1), 4),
      time_id = time_id,
      .groups = "drop"
    )
}

# ------------------- Compute All Horizons -------------------
results <- bind_rows(
  summarise_horizon(unpacked, 526:528, 3),
  summarise_horizon(unpacked, 526:531, 6),
  summarise_horizon(unpacked, 526:537, 12)
)

# ------------------- Ensure Data Types -------------------
results <- results %>%
  mutate(
    priogrid_gid = as.integer(priogrid_gid),
    time_id = as.integer(time_id),
    outcome_n = as.integer(outcome_n),
    outcome_p = as.numeric(outcome_p)
  )

# ------------------- Write Output -------------------
out_dir <- file.path(draws_dir, "submission")
dir.create(out_dir, showWarnings = FALSE)

out_file <- file.path(out_dir, paste0("horisoned_submission_", mode, ".parquet"))
write_parquet(results, out_file)

cat("Saved submission file to:", out_file, "\n")
