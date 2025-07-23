
data_start_date <- as.Date("2010-01-01")
data_end_date   <- as.Date("2023-06-01")   # last predictors
forecast_date   <- as.Date("2023-10-01")   # s = 1 target month
validation_months <- 24
horizon_max       <- 12                    # longest forecast horizon
s_list            <- c(1, 6, 12)           # horizons you want to inspect

# 2. Helper converters
to_mid <- function(d) 12*(as.integer(format(d, "%Y"))-1980L) + as.integer(format(d,"%m"))
from_mid <- function(m) as.Date(sprintf("%04d-%02d-01", 1980L+(m-1)%/%12L, 1L+(m-1)%%12L))

# 3. Derive predictor windows (unchanging for all horizons)
start_mid  <- to_mid(data_start_date)
end_mid    <- to_mid(data_end_date)              # test predictors
val_end    <- end_mid - horizon_max              # 12-month buffer
val_start  <- val_end - (validation_months-1L)
train_end  <- val_start - 1L
train_start<- start_mid

cat(sprintf("Predictor windows:\n  Train X : %s → %s\n  Valid X : %s → %s\n  Test  X : %s\n\n",
            format(from_mid(train_start),"%Y-%m"), format(from_mid(train_end),"%Y-%m"),
            format(from_mid(val_start),  "%Y-%m"), format(from_mid(val_end),  "%Y-%m"),
            format(from_mid(end_mid),    "%Y-%m")))

# 4. Show DV ranges for each horizon s
base_shift <- to_mid(forecast_date) - end_mid    # 4 in this setup
cat("Horizon  Shift | Train-DV range            | Valid-DV range            | Test-DV\n")
for (s in s_list) {
  shift <- base_shift+ (s-1)                    # s→shift relation
  train_dv_start <- train_start + shift
  train_dv_end   <- train_end   + shift
  val_dv_start   <- val_start   + shift
  val_dv_end     <- val_end     + shift
  test_dv        <- end_mid     + shift
  cat(sprintf("   %2d     %3d | %s → %s | %s → %s | %s\n",
      s, shift,
      format(from_mid(train_dv_start),"%Y-%m"), format(from_mid(train_dv_end),"%Y-%m"),
      format(from_mid(val_dv_start),  "%Y-%m"), format(from_mid(val_dv_end),  "%Y-%m"),
      format(from_mid(test_dv),       "%Y-%m")))
}
