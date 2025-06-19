library(sf)
library(data.table)
library(arrow)
library(ggplot2)

# 1. LOAD PREDICTION DATA
pred_dt <- as.data.table(read_parquet("data/forecasts/GBM_2_AutoML_1_20250617_164332h4_202310_1_526.parquet"))
pred_dt <- pred_dt[, .(
  gid = as.character(priogrid_gid), 
  pred_value = predict
  )]

# 2. LOAD GRIDS
grids <- st_read("data/pgm/priogrid_cell.shp", quiet = TRUE)
grids_dt <- as.data.table(grids)[, .(gid = as.character(gid), geometry)]
grids_dt <- grids_dt[gid %chin% unique(pred_dt$gid)]
grids_sf <- st_as_sf(grids_dt)

# 3. MERGE PREDICTIONS
grid_pred <- merge(grids_dt, pred_dt, by = "gid", all = FALSE) |> st_as_sf()
# 3. MERGE PREDICTIONS


# 4. LOAD COUNTRIES
countries <- st_read("data/pgm/cshapes.shp", quiet = TRUE)[c("GWCODE", "CNTRY_NAME")]
countries <- st_transform(countries, st_crs(grid_pred))

# 5. ZOOM INTO AFRICA (by filtering country geometry bounds)
africa_bbox <- st_bbox(c(xmin = -20, xmax = 59, ymin = -40, ymax = 40), crs = st_crs(grid_pred))

# 6. PLOT: LIKE YOUR SECOND IMAGE
ggplot() +
  geom_sf(data = grid_pred, aes(fill = pred_value), color = "black", size = 0.01) +
  geom_sf(data = countries, fill = NA, color = "black", linewidth = 0.3) +
  scale_fill_viridis_c(
  
  
  limits = c(0, max(grid_pred$pred_value, na.rm = TRUE)),  # set 0 as minimum
  name = "Prediction",
  na.value = "white"  # show NAs as white, NOT zero values
) +
  coord_sf(xlim = c(africa_bbox["xmin"], africa_bbox["xmax"]),
           ylim = c(africa_bbox["ymin"], africa_bbox["ymax"])) +
  theme_minimal() +
  labs(title = "Prediction Heatmap over Africa")

# 7. SAVE
ggsave("final_prediction_africa_zoom.png", width = 10, height = 10, dpi = 300, bg = "white")
