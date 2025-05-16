# views1.R
# the first r script for forecasting at the grid level

# to get the most recent h2o: https://h2o-release.s3.amazonaws.com/h2o/rel-3.46.0/7/index.html

# The following two commands remove any previously installed H2O packages for R.
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Next, we download packages that H2O depends on.
#pkgs <- c("RCurl","jsonlite")
#for (pkg in pkgs) {
#  if (! (pkg %in% rownames(installed.packages()))) { install.packages(pkg) }
#}

# Now we download, install and initialize the H2O package for R.
#install.packages("h2o", type="source", repos="https://h2o-release.s3.amazonaws.com/h2o/rel-3.46.0/7/R")

rm(list=ls())

# for reading data
library(arrow)
library(data.table)


# load H2O and start up an H2O cluster
library(h2o)
h2o.init(max_mem_size = "32g") #is this needed for xgboost?

# for boxcox transformations
library(DescTools)

setwd("/Users/vjdorazio/Desktop/github/forecasting-fast")

# source utils script
source("views_utils.R")

# set to sb
myvars <- "sb"

# set to one of -1, -.5, 0, .5, 1
lambda <- 0

# how many months out? takes on value of 1-12
shift <- 1

# how many models for autoML?
mm <- 10

# how long to run h2o in seconds?
h2oruntime <- 120

# read parquet data
mydata <- read_parquet("/Users/vjdorazio/Desktop/github/forecasting/data/views2023/pgm_features_to_oct2017.parquet")

# adding 2018
adddata <- read_parquet("/Users/vjdorazio/Desktop/github/forecasting/data/views2023/pgm_features_to_oct2018.parquet")
mydata <- rbind(mydata, adddata)

# adding 2019
adddata <- read_parquet("/Users/vjdorazio/Desktop/github/forecasting/data/views2023/pgm_features_to_oct2019.parquet")
mydata <- rbind(mydata, adddata)

# adding 2020
adddata <- read_parquet("/Users/vjdorazio/Desktop/github/forecasting/data/views2023/pgm_features_to_oct2020.parquet")
mydata <- rbind(mydata, adddata)

rm(adddata)

# keep only the SB variables
if(myvars=="sb") {
  keeps <- getvars(v="_sb", df=mydata)
  mydata <- mydata[,keeps]
}

df <- builddv(s=shift, df=mydata)

# check the shift with grid 178273 because it has SB values
#temp <- df[which(df$priogrid_gid==178273),c("priogrid_gid", "month_id", "pred_month_id", "ged_sb", "pred_ged_sb")]
#temp <- df[df$priogrid_gid=="178273",c("priogrid_gid", "month_id", "pred_month_id", "ged_sb", "pred_ged_sb")]
#temp <- as.data.frame(temp)

# set factors
df$priogrid_gid <- as.factor(df$priogrid_gid)

# before omitting, partition to save the latest month_id (454)
maxmonth <- max(df$month_id)
forecastdata <- df[which(df$month_id==maxmonth),]
forecastmonth <- maxmonth + shift + 2 # CHECK THIS
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

df <- as.h2o(df)
mydv <- "y"

if(myvars=="sb") {
  predictors <- colnames(df)[c(3:36)]
}

# should be TRUE
h2o.isnumeric(df[mydv])

## autoML training
aml <- h2o.automl(x=predictors, y=mydv, training_frame = df, max_models = mm, seed=4422, max_runtime_secs = h2oruntime)

u <- aml@leaderboard$model_id

# note that aml@leader is the model, the filename is the model_id which i believe is unique
h2o.saveModel(object=aml@leader, path="/Users/vjdorazio/Desktop/github/forecasting-fast/models")

leader <- aml@leader
# if needing to go back and load a model
# just the leader is saved, and this loads the leader
#leader <- h2o.loadModel("/Users/vjdorazio/Desktop/github/forecasting/models/StackedEnsemble_AllModels_1_AutoML_12_20230915_220213")

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

# write the predictions
out <- as.data.frame(preds)
df <- df[,c("priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")]
temp <- as.data.frame(df) 
out <- cbind(out, temp)

fn <- paste("/Users/vjdorazio/Desktop/github/forecasting-fast/data/predictions/", leader@model_id, ".parquet", sep='')
write_parquet(out, fn)

# write the forecasts
out <- as.data.frame(forecasts)
forecastdata <- forecastdata[,c("priogrid_gid", "month_id", "ged_sb", "pred_ged_sb", "pred_month_id")]
temp <- as.data.frame(forecastdata) 
out <- cbind(out, temp)

fn <- paste("/Users/vjdorazio/Desktop/github/forecasting-fast/data/forecasts/", leader@model_id, "_", forecastmonth, ".parquet", sep='')
write_parquet(out, fn)

h2o.removeAll()









