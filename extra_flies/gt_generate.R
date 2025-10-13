# ────────────────────────────────────────────────────────────────────
# gt_generate.R  — build ground-truth snapshots + zero-count check
# -------------------------------------------------------------------
suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
  library(lubridate)
  library(glue)
})

## 1 ▸ working dir ---------------------------------------------------
proj_root <- "/users/ps00068/forecasting-fast"
if (!grepl("forecasting-fast$", getwd()))
  setwd(proj_root)

## 2 ▸ paths ---------------------------------------------------------
truth_parquet <- "data/pgm/pgm_data_121_542.parquet"
dates_txt     <- "forecast_dates.txt"
out_dir       <- "data/gt_snapshots"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

## 3 ▸ constants -----------------------------------------------------
data_end_date <- as.Date("2023-06-01")
data_end_mid  <- 12L * (year(data_end_date) - 1980L) + month(data_end_date)

## 4 ▸ forecast months ----------------------------------------------
forecast_dates <- as.Date(trimws(readLines(dates_txt)))
if (any(is.na(forecast_dates)))
  stop(" Non-date entry in forecast_dates.txt")
forecast_mids <- 12L * (year(forecast_dates) - 1980L) + month(forecast_dates)

## 5 ▸ Arrow dataset & persistence lookup ---------------------------
ds_truth <- open_dataset(truth_parquet)

persist_dt <- ds_truth %>%
  filter(month_id == data_end_mid) %>%
  select(priogrid_gid, ged_sb) %>%
  collect() %>%
  as.data.table()
if (!nrow(persist_dt)) stop("  No rows for persistence anchor 2023-06.")
setnames(persist_dt, c("priogrid_gid", "ged_sb"),
                     c("priogrid_gid", "persist_pred"))

## 6 ▸ helper --------------------------------------------------------
write_snapshot <- function(mid) {
  out_file <- file.path(out_dir, glue("gt_{mid}.parquet"))
  if (file.exists(out_file)) {
    message("✓  ", basename(out_file), " already exists – skipping.")
    return(invisible())
  }

  gt_dt <- ds_truth %>%
    filter(month_id == mid) %>%
    select(priogrid_gid, ged_sb) %>%
    collect() %>%
    as.data.table()
  if (!nrow(gt_dt)) {
    warning(" No ground-truth rows for month_id = ", mid, ". Snapshot skipped.")
    return(invisible())
  }
  setnames(gt_dt, c("priogrid_gid", "ged_sb"), c("priogrid_gid", "gt"))

  snap <- merge(gt_dt, persist_dt, by = "priogrid_gid", all.x = TRUE)
  snap[is.na(persist_pred), persist_pred := 0]
  snap[, `:=`(zero_pred = 0L, month_id = mid)]
  snap <- snap[, .(priogrid_gid, gt, zero_pred, persist_pred, month_id)]

  ## zero-count diagnostics -----------------------------------------
  z_gt       <- sum(snap$gt            == 0, na.rm = TRUE)
  z_zero     <- sum(snap$zero_pred     == 0)            # should equal nrow
  z_persist  <- sum(snap$persist_pred  == 0, na.rm = TRUE)

  if (z_zero != nrow(snap))
    warning("zero_pred column in ", basename(out_file), " is not all zeros!")

  message(sprintf(
    "✔  wrote %s (%d rows)  | zeros → gt:%d  zero_pred:%d  persist_pred:%d",
    basename(out_file), nrow(snap), z_gt, z_zero, z_persist))

  write_parquet(snap, out_file)
}

## 7 ▸ run for every forecast month ---------------------------------
invisible(lapply(forecast_mids, write_snapshot))

message("\n  All snapshots saved to: ", normalizePath(out_dir), "\n")
