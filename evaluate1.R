# ─── compare_one_month.R  (Africa zoom • MID titles • correct mask) ─
suppressPackageStartupMessages({
  library(arrow);  library(data.table); library(sf)
  library(ggplot2); library(patchwork); library(stringr); library(glue)
})

## 1 ▸ forecast parquet path from CLI
args <- commandArgs(trailingOnly = TRUE)
if (!length(args))
  stop("Usage:  Rscript compare_one_month.R <forecast.parquet>")
forecast_file <- args[1]
if (!file.exists(forecast_file))
  stop("File not found: ", forecast_file)

## 2 ▸ extract MID = last 3 digits before “.parquet”
mid <- as.integer(
         str_match(basename(forecast_file), "_(\\d{3})\\.parquet$")[, 2])
if (is.na(mid))
  stop("Couldn’t parse MID (last-3-digit) from filename.")

gt_file <- file.path("data/gt_snapshots", paste0("gt_", mid, ".parquet"))
if (!file.exists(gt_file))
  stop("Ground-truth snapshot not found: ", gt_file)

## 3 ▸ LOAD tables
pred_dt <- as.data.table(read_parquet(forecast_file))[
             , .(priogrid_gid = as.character(priogrid_gid),
                 pred_value   = predict)]

gt_dt   <- as.data.table(read_parquet(gt_file))[
             , .(priogrid_gid = as.character(priogrid_gid),
                 gt, zero_pred, persist_pred)]

grids <- st_read("data/pgm/priogrid_cell.shp", quiet = TRUE)
grids_dt <- as.data.table(grids)[
              , .(priogrid_gid = as.character(gid), geometry)]

countries <- st_read("data/pgm/cshapes.shp", quiet = TRUE)[, c("GWCODE","CNTRY_NAME")]
countries <- st_transform(countries, st_crs(grids))

## 3½ ▸  keep only polygons that appear in the forecast  ← key change
grids_dt <- grids_dt[priogrid_gid %chin% pred_dt$priogrid_gid]

## 4 ▸ MERGE into one sf
eval_dt <- merge(grids_dt, gt_dt,   by = "priogrid_gid", all.x = TRUE)
eval_dt <- merge(eval_dt,  pred_dt, by = "priogrid_gid", all.x = TRUE) |>
           st_as_sf()

## 5 ▸ RMSEs
rmse <- function(truth, pred) sqrt(mean((truth - pred)^2, na.rm = TRUE))
rmse_pred <- rmse(eval_dt$gt,            eval_dt$pred_value)
rmse_zero <- rmse(eval_dt$gt,            eval_dt$zero_pred)
rmse_pers <- rmse(eval_dt$gt,            eval_dt$persist_pred)

## 6 ▸ colour scale
lim_hi <- max(eval_dt$gt, eval_dt$pred_value,
              eval_dt$persist_pred, na.rm = TRUE)
fill_scale <- scale_fill_viridis_c(limits = c(0, lim_hi),
                                   na.value = "white", name = "Count")

## 7 ▸ helper for one panel  (Africa zoom + MID in title)
mk_panel <- function(fill_col, ttl) {
  ggplot(eval_dt) +
    geom_sf(aes(fill = !!sym(fill_col)), colour = NA) +
    geom_sf(data = countries, fill = NA, colour = "black", linewidth = .25) +
    fill_scale +
    coord_sf(xlim = c(-20, 59), ylim = c(-40, 40)) +     # Africa clip
    theme_void(base_size = 9) +
    labs(title = glue("{ttl}  (MID {mid})"))
}

p1 <- mk_panel("gt",          "Observed (gt)\nRMSE ref")
p2 <- mk_panel("pred_value",  glue("Forecast\nRMSE {sprintf('%.3f', rmse_pred)}"))
p3 <- mk_panel("zero_pred",   glue("All-Zero\nRMSE {sprintf('%.3f', rmse_zero)}"))
p4 <- mk_panel("persist_pred",glue("Persistence\nRMSE {sprintf('%.3f', rmse_pers)}"))

## 8 ▸ assemble & save (still a single figure)
out_png <- file.path("compare_plots", paste0("compare_", mid, ".png"))
dir.create(dirname(out_png), recursive = TRUE, showWarnings = FALSE)

ggsave(out_png, plot = (p1 | p2) / (p3 | p4),
       width = 9, height = 7, dpi = 300, bg = "white")

cat("\n Figure saved:", normalizePath(out_png), "\n",
    "RMSE – Forecast:", round(rmse_pred,3),
    "| Zero:", round(rmse_zero,3),
    "| Persistence:", round(rmse_pers,3), "\n")

