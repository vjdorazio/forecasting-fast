#!/usr/bin/env Rscript

# views1_dub_hpc2024.R
# script uses SB features and H2O

# read in the arguments
args = commandArgs(trailingOnly = TRUE)
runname <- args[2]
args <- as.numeric(args[1])

# set this to false to run again, after the model json file has been written,
# to try to estimate the models it missed the first time
# then just ./runall.sh viewsfinal1 again, it'll skip if the model is already there
firstrun <- FALSE

## for testing
#args <- 1
#runname <- 'test2024_1'
#runname <- 'viewsfinal1'
##

portseq <- seq(from=3, to=1500, by=3)
myport <- 54320+portseq[args]

lambdavals <- c(-1, -.5, 0, .5, 1)
shiftvals <- 1:12
h2otimevals <- 9000
yearsvals <- 2017:2022

vals <- expand.grid(lambdavals, shiftvals, yearsvals, h2otimevals)

lambda <- vals[args,1]
shift <- vals[args,2]
years <- vals[args,3]
h2otime <- vals[args,4]

# how to check if i already have the model?
# read in all the jsons?
if(firstrun == FALSE) {
  library(jsonlite)
  library(arrow)
  library(data.table)
  fd <- paste("/scratch/vjd00006/forecasting/data/forecasts/", runname, sep='')
  ff <- list.files(fd)
  ff <- ff[which(substr(ff, nchar(ff)-7, nchar(ff))=='.parquet')]
  d <- nchar("_000.parchet")
  
  for(i in 1:length(ff)) {
    modelname <- substr(ff[i], 1, nchar(ff[i])-d)
    modeljson <- paste('/scratch/vjd00006/forecasting/models/', modelname, '.json', sep='')
    myjson <- read_json_arrow(modeljson)
    if(myjson$years[[1]]==years & myjson$shift[[1]]==shift & myjson$lambda[[1]]==lambda) {
      print(paste('Model ', modelname, ' is for year ', years, ', shift ', shift, ', and lambda ', lambda, sep=''))
      quit()
    }
  }
}

## again for testing
#lambda <- 0
#h2otime <- 120
#years <- 2022
##

print('dont have these!!')
print("lambda shift years h2otime")
print(lambda)
print(shift)
print(years)
print(h2otime)


# this will drop the priogrid ID as a factor variable in predictors
nogrid <- TRUE

# this will drop the country ID as a factor variable in predictors
nocid <- TRUE

library(jsonlite)

library(arrow)
library(data.table)


# load H2O and start up an H2O cluster
library(h2o)
h2o.init(port=myport, nthreads=5) #is max_mem needed for xgboost?

# for boxcox transformations
library(DescTools)

setwd("/scratch/vjd00006/forecasting")

# source utils script
source("views_utils.R")

# set to one of sb, ged, acled, sbacled, sbplus
myvars <- "sbplus"

# how many models for autoML?
mm <- 20

# read parquet data
mydata <- data.table(read_parquet("/scratch/vjd00006/forecasting/data/views2024/pgm_features_to_oct2017.parquet"))
l <- list(mydata)

# adding 2018
if(years>2017) {
  adddata <- data.table(read_parquet("/scratch/vjd00006/forecasting/data/views2024/pgm_features_to_oct2018.parquet"))
  l <- list(mydata, adddata)
  mydata <- rbindlist(l, use.names=TRUE)
  #mydata <- rbind(mydata, adddata)
  rm(adddata)
  gc()
}

# adding 2019
if(years>2018) {
  adddata <- data.table(read_parquet("/scratch/vjd00006/forecasting/data/views2024/pgm_features_to_oct2019.parquet"))
  l <- list(mydata, adddata)
  mydata <- rbindlist(l, use.names=TRUE)
  #mydata <- rbind(mydata, adddata)
  rm(adddata)
  gc()
}

# adding 2020
if(years>2019) {
  adddata <- data.table(read_parquet("/scratch/vjd00006/forecasting/data/views2024/pgm_features_to_oct2020.parquet"))
  l <- list(mydata, adddata)
  mydata <- rbindlist(l, use.names=TRUE)
  #mydata <- rbind(mydata, adddata)
  rm(adddata)
  gc()
}

# adding 2021
if(years>2020) {
  adddata <- data.table(read_parquet("/scratch/vjd00006/forecasting/data/views2024/pgm_features_to_oct2021.parquet"))
  l <- list(mydata, adddata)
  mydata <- rbindlist(l, use.names=TRUE)
  #mydata <- rbind(mydata, adddata)
  rm(adddata)
  gc()
}

# adding 2022
if(years>2021) {
  adddata <- data.table(read_parquet("/scratch/vjd00006/forecasting/data/views2024/pgm_features_to_oct2022.parquet"))
  l <- list(mydata, adddata)
  mydata <- rbindlist(l, use.names=TRUE)
  #mydata <- rbind(mydata, adddata)
  rm(adddata)
  gc()
}

mydata <- as.data.frame(mydata)

# keep only the SB variables
if(myvars=="sb" | myvars=="sbacled" | myvars=="sbplus") {
  keeps <- getvars(v="_sb", df=mydata)
  mydata <- mydata[,keeps]
}

if(myvars=="ged" | myvars=="acled") {
  # if adding ns and os
  keeps <- getvars(v="_sb", df=mydata)
  keepsns <- getvars(v="_ns", df=mydata)
  keepsos <- getvars(v="_os", df=mydata)
  keeps <- unique(c(keeps, keepsns, keepsos))
  mydata <- mydata[,keeps]
}

if(myvars=="acled" | myvars=="sbacled") {
  # if also adding acled
#  acled <- fread("data/dt_total_fatalities.csv")
  acled <- fread("data/transform1.csv")
  # i checked and yrmo 2017-10 is month_id 454, which is same for prio so that's what we want
  acledkeeps <- c("gid", "month_id", "acled_stratdev_sum", "acled_stratdev_count", "acled_riots_sum", "acled_riots_count", 
                  "acled_protests_sum", "acled_protests_count", "acled_civvio_sum", "acled_civvio_count", 
                  "acled_remote_sum", "acled_remote_count", "acled_battles_sum", "acled_battles_count")
  acled <- acled[,..acledkeeps]
  acled <- as.data.frame(acled)
  
  # recoding NAs to 0
  for(ai in 3:ncol(acled)) {
    acled[which(is.na(acled[,ai])),ai] <- 0
  }
  
  mydata <- merge(mydata, acled, by.x=c("priogrid_gid", "month_id"), by.y=c("gid", "month_id"), all.x=TRUE)
  rm(acled)
}


if(myvars=="sbplus") {
  sbplus <- fread("data/GED/dt_sb2024.csv")
 # keepsum <- getvars(v="_sum", df=sbplus)
  keepscount <- getvars(v="_count", df=sbplus)
#  keepsdummy <- getvars(v="_dummy", df=sbplus)
 # keeps <- unique(c(keepsum, keepscount, keepsdummy))
  keeps <- unique(c(keepscount))
  sbplus <- sbplus[,..keeps]
  sbplus <- as.data.frame(sbplus)
  
  # recoding NAs to 0... necessary?
  for(ai in 1:ncol(sbplus)) {
    sbplus[which(is.na(sbplus[,ai])),ai] <- 0
  }
  
  mydata <- merge(mydata, sbplus, by=c("priogrid_gid", "month_id"), all.x=TRUE)
  rm(sbplus)
}

df <- builddv(s=shift, df=mydata)

# check the shift with grid 178273 because it has SB values
#temp <- df[which(df$priogrid_gid==178273),c("priogrid_gid", "month_id", "pred_month_id", "ged_sb", "pred_ged_sb")]
#temp <- df[df$priogrid_gid=="178273",c("priogrid_gid", "month_id", "pred_month_id", "ged_sb", "pred_ged_sb")]
#temp <- as.data.frame(temp)

# adding in the states
df <- addstates(df)

# set factors
df$gwno <- as.factor(df$gwno)
df$priogrid_gid <- as.factor(df$priogrid_gid)

# before omitting, partition to save the latest month_id (was 454 for 2017, now 514 for 2022)
maxmonth <- max(df$month_id)
forecastdata <- df[which(df$month_id==maxmonth),]
forecastmonth <- maxmonth + shift + 2 # CHECK THIS (been checked but check again)
forecastdata$pred_month_id <- forecastmonth

# omit NA because shift will create NAs
df <- na.omit(df)

# check
range(df$month_id)
range(df$pred_month_id)

# drop if the DV has negative values (it shouldn't)
df <- df[which(df$pred_ged_sb>=0),]

# Box-Cox transformation
df$y <- BoxCox(df$pred_ged_sb+1, lambda=lambda)

# if lambda is 1, this should be true
all(df$y==df$pred_ged_sb)

print("HERE168")

df <- as.h2o(df)
mydv <- "y"
gc()

# ALL NEEDS REVISION BECAUSE INDEX SHIFTED (checked, but keep checking)
# this could be moved to utils
if(myvars=="sb") {
  # predictors if sb
  # NOTE: 9/15 decided to drop month_id BUT CHECK TO SEE WHATS BETTER
  predictors <- colnames(df)[c(1, 3:36, 39)]
}
if(myvars=="sbplus") {
  # predictors if sbplus
  # NOTE: 9/15 decided to drop month_id BUT CHECK TO SEE WHATS BETTER
  predictors <- colnames(df)[c(1, 3:74, 77)]
}
if(myvars=="ged") {
  # predictors if sb, ns, and os
  predictors <- colnames(df)[c(1:66, 70)]
}
if(myvars=="acled") {
  # predictors if sb, ns, os, and acled
  predictors <- colnames(df)[c(1:78, 82)]
}
if(myvars=="sbacled") {
  # predictors if sb and acled
  predictors <- colnames(df)[c(1:48, 52)]
}

print("HERE196")

# should both be TRUE
h2o.isnumeric(df[mydv])
h2o.isfactor(df["gwno"])
h2o.isfactor(df["priogrid_gid"])

if(nogrid) {
  predictors <- predictors[-which(predictors=="priogrid_gid")]
}

if(nocid) {
  predictors <- predictors[-which(predictors=="gwno")]
}

## autoML training
aml <- h2o.automl(x=predictors, y=mydv, training_frame = df, max_models = mm, seed=4422, max_runtime_secs = h2otime)
#aml <- h2o.automl(x=predictors, y=mydv, training_frame = df, seed=4422)              
gc()

# i wonder if helps to specify the distribution as tweedie? or gamma? or poisson?
# also wonder if it helps to change the sort_metric? that is basically how the leader is selected
u <- aml@leaderboard$model_id

# note that aml@leader is the model, the filename is the model_id which i believe is unique
h2o.saveModel(object=aml@leader, path="/scratch/vjd00006/forecasting/models")

leader <- aml@leader
# if needing to go back and load a model
# just the leader is saved, and this loads the leader
#leader <- h2o.loadModel("/Users/vjdorazio/Desktop/github/forecasting/models/StackedEnsemble_AllModels_1_AutoML_12_20230915_220213")

# for 493
#leader <- h2o.loadModel("/Users/vjdorazio/Desktop/github/forecasting/models/StackedEnsemble_AllModels_1_AutoML_1_20230921_163922")

preds <- h2o.predict(leader, newdata=df) # predictions on the training data

# needs to be as.h2o
forecastdata <- as.h2o(forecastdata)
forecasts <- h2o.predict(leader, newdata=forecastdata) # forecasts using the latest month of data (454)


# inverse the box-cox transformation to return to counts
# this could be sent to utils
m <- as.matrix(preds)
mf <- as.matrix(forecasts)

# when  lambda is -1, BoxCoxInv will set anything greater than or equal to 1 to NA
# as a default, i'll set them equal to the largest prediction less than 1
if(lambda==-1) {
  val <- m[which(m<1),1]
  val <- max(val)
  m[which(m>=1),1] <- val 
  
  # now for forecasts
  val <- mf[which(mf<1),1]
  val <- max(val)
  mf[which(mf>=1),1] <- val 
}

preds <- BoxCoxInv(m, lambda=lambda) - 1
forecasts <- BoxCoxInv(mf, lambda=lambda) - 1

print("HERE252")

# write the predictions
out <- as.data.frame(preds)
df <- df[,c("gwno", "priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")]
temp <- as.data.frame(df) 
out <- cbind(out, temp)

predictpath <- paste("/scratch/vjd00006/forecasting/data/predictions/", leader@model_id, ".parquet", sep='')
write_parquet(out, predictpath)

# write the forecasts
out <- as.data.frame(forecasts)
forecastdata <- forecastdata[,c("gwno", "priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")]
temp <- as.data.frame(forecastdata) 
out <- cbind(out, temp)

forecastpath <- paste("/scratch/vjd00006/forecasting/data/forecasts/", leader@model_id, "_", forecastmonth, ".parquet", sep='')
write_parquet(out, forecastpath)

# write out model json info
outlist <- list("runname"=runname, "years"=years, "shift"=shift, "lambda"=lambda, "h2otime"=h2otime, "model_id"=leader@model_id,"forecastmonth"=forecastmonth, "forecastpath"=forecastpath, "predictpath"=predictpath, "predictors"=predictors)
fn <- paste("/scratch/vjd00006/forecasting/models/", leader@model_id, ".json", sep='')
write_json(x=outlist, path=fn)


## delete below this
#test <- read_parquet("data/forecasts/StackedEnsemble_BestOfFamily_1_AutoML_1_20240612_172831_509.parquet")


