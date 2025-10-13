
rm(list=ls())
library(arrow)
library(dplyr)

base_dir <- "~/scratch/forecasting-fast/data/debug_data"
modes <- c("new")#c("tlags", "specific")

for (mode in modes) {
  mode_dir <- file.path(base_dir, mode)
  
  files <- list.files(mode_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
  dfs <- lapply(files, read_parquet)
  df_combined <- bind_rows(dfs)
  
  combined_path <- file.path(mode_dir, paste0("baseline_combined_2022growseas", mode, ".parquet"))
  write_parquet(df_combined, combined_path)
  cat("Wrote combined file:", combined_path, "\n")
  
  df_filtered <- df_combined %>%
    filter(month_id == 546) %>%
    transmute(
      priogrid_gid = as.integer(priogrid_gid),
      month_id     = as.integer(pred_month_id),
      outcome_n    = as.integer(outcome_n),
      outcome_p    = as.numeric(outcome_p),
      time_id      = as.integer(pred_month_id - 546-3)
    )
  
  monthly_submission_path <- file.path(mode_dir, paste0("monthly_submission_2025__2022growseas", mode, ".parquet"))
  write_parquet(df_filtered %>% select(-time_id), monthly_submission_path)
  cat("Wrote:", monthly_submission_path, "\n")
  
  three_six_twelve_path <- file.path(mode_dir, paste0("3-6-12_submission_2025__2022growseas", mode, ".parquet"))
  write_parquet(
    df_filtered %>% filter(time_id %in% c(3L, 6L, 12L)),
    three_six_twelve_path
  )
  cat("Wrote:", three_six_twelve_path, "\n")
}