# views3_dub_hpc.R
# reads in: (1) point forecasts and (2) grid-specific parameterizations
# takes 1000 draws for the forecast distribution
# writes the views-formatted output

rm(list=ls())

library(arrow)
library(tweedie)

# setwd("/Users/vjdorazio/Desktop/github/forecasting-fast")  # COMMENTED: not needed for SLURM

# source utils script
source("views_utils.R")

# dir <- "/Users/vjdorazio/Desktop/github/forecasting-fast/data/forecasts/"    
# ff <- list.files(dir)                                                       
# ff <- unique(grep(paste(".parquet",collapse="|"), ff, value=TRUE))          

# fd <- paste("params/t_", ff, sep='')                                        
# d <- nchar("_000.parchet")                                                  

n <- 1000 # draws for views

smidge <- .00001

set.seed(4422)

#---------<>----------
# Accept forecast file path as argument (for SLURM job)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript views3.R <forecast_file>")
}
forecast_file <- args[1]

# Identify hp_tag (hyperparameter directory) and scratch root
hp_tag <- basename(dirname(forecast_file))
scratch_root <- file.path(path.expand("~"), "scratch", "forecasting-fast")
#---------<>----------

#---------<>----------
# Build correct params file path (handle flipped order and strip forecastmonth)
forecast_name <- tools::file_path_sans_ext(basename(forecast_file))
forecast_base <- sub("_[0-9]+$", "", forecast_name)  # drop the final _<forecastmonth>

parts <- strsplit(forecast_base, "_h", fixed = TRUE)[[1]]
model_id <- parts[1]
run_label <- paste0("h", parts[2])  # add back the "h"

params_basename <- paste0(run_label, "_", model_id, "_params.parquet")
params_file <- file.path(scratch_root, "data", "params", hp_tag, params_basename)
#---------<>----------

#---------<>----------
# Log which forecast and params file is processed (for SLURM logs)
cat("\n=========================================\n")
cat("Processing forecast/params pair:\n")
cat("  Forecast file :", forecast_file, "\n")
cat("  Params file   :", params_file, "\n")
cat("=========================================\n\n")
#---------<>----------

#    print(i)
# i <- 1 # removing the old for loop                                     
# fn <- paste(dir, ff[i], sep='')                                       
#  # read forecast data and code < 0 to 0
# mydata <- read_parquet(fn)                                            
# mydata$predict[which(mydata$predict<=0)] <- smidge                    

# outdata <- mydata[,c("pred_month_id", "priogrid_gid", "predict")]     
# outdata$draw <- list(rep(-1, 1000))                                   

# fp <- paste(dir, fd[i], sep='')                                       
# fp <- substr(fp, 1, nchar(fp)-d)                                     
# fp <- paste(fp, ".parquet", sep='')                                   

#---------<>----------
# New data loading
mydata <- read_parquet(forecast_file)
myparamsdata <- read_parquet(params_file)

# Deduplicate params (drop the 24 monthly duplicates per grid)
#myparamsdata <- myparamsdata[!duplicated(myparamsdata$priogrid_gid), ] # not needed now its handled in views2

# Prepare forecast data (replace negatives, build output skeleton)
mydata$predict[which(mydata$predict<=0)] <- smidge

# old code that lists draws in one row
# outdata <- mydata[,c("pred_month_id", "priogrid_gid", "predict")]
# outdata$draw <- list(rep(-1, 1000))
# #---------<>----------
# 
# # Generate draws (unchanged loop)
# for(ii in 1:nrow(mydata)) {
#   myparams <- myparamsdata[which(myparamsdata$priogrid_gid==mydata$priogrid_gid[ii]),]
#   draws <- round(rtweedie(n=n, power=myparams$power[1], mu=mydata$predict[ii], phi=myparams$theta[1]), digits=0)
#   outdata$draw[ii] <- list(draws)
# }
# 
# # Rename columns to match "views" format
# colnames(outdata) <- c("month_id", "priogrid_gid", "outcome", "draw")
# 
# #---------<>----------
# # Save to scratch draws directory with correct naming
# draws_dir <- file.path(scratch_root, "data", "draws", hp_tag)
# dir.create(draws_dir, recursive = TRUE, showWarnings = FALSE)
# output_file <- file.path(draws_dir, paste0(forecast_name, "_draws.parquet"))
# write_parquet(outdata, output_file)
# 
# cat("Wrote draws to:", output_file, "\n")
#---------<>----------


draw_rows <- list()

for (ii in 1:nrow(mydata)) {
  myparams <- myparamsdata[which(myparamsdata$priogrid_gid == mydata$priogrid_gid[ii]), ]
  
  draws <- round(rtweedie(n = n, power = myparams$power[1], mu = mydata$predict[ii], phi = myparams$theta[1]), digits = 0)
  
  draw_df <- data.frame(
    month_id = rep(mydata$pred_month_id[ii], n),
    priogrid_gid = rep(mydata$priogrid_gid[ii], n),
    draw = 1:n,
    outcome = draws
  )
  
  draw_rows[[ii]] <- draw_df
}

flat_outdata <- do.call(rbind, draw_rows)

# Save flat format
draws_dir <- file.path(scratch_root, "data", "draws", hp_tag)
dir.create(draws_dir, recursive = TRUE, showWarnings = FALSE)
flat_output_file <- file.path(draws_dir, paste0(forecast_name, "_draws.parquet"))
write_parquet(flat_outdata, flat_output_file)

cat("Wrote flat draw format to:", flat_output_file, "\n")
#=======================================================


# fout <- "/Users/vjdorazio/Desktop/github/forecasting-fast/data/draw/draw"  
# xt <- substr(ff[i], nchar(ff[i])-d+1, nchar(ff[i]))                        
# fout <- paste(fout, xt, sep='')                                           
# write_parquet(outdata, fout)                                               
