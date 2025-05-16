# views2_dub_hpc.R

# reads in a parquet file, the name is the model ID
# optimizes the cutoff and the parameterizations of distributions for CRPS
# built for tweedie
# moving window of 5 years for distributions
# 

library(scoringRules)
library(MASS) # using this for rnegbin
library(tweedie) # for the tweedie

# for reading data
library(arrow)

setwd("/Users/vjdorazio/Desktop/github/forecasting-fast")

# source utils script
source("views_utils.R")

dir <- "/Users/vjdorazio/Desktop/github/forecasting-fast/data/forecasts/"
ff <- list.files(dir)
ff <- unique(grep(paste(".parquet",collapse="|"), ff, value=TRUE))

# this is for the forecasts, if month_id is over 999 this won't work
# that's something like 40 years from now
d <- nchar("_000.parchet")

# just start with 1 without looping
#for(a in 2:length(ff)) {
#  Sys.sleep(60) # take a break
#  print(a)
fp <- substr(ff[1], 1, nchar(ff[1])-d)
fp <- paste(fp, ".parquet", sep='')
dir <- "/Users/vjdorazio/Desktop/github/forecasting-fast/data/predictions/"
fn <- paste(dir, fp, sep='')

# read parquet data
mydata <- read_parquet(fn)

# keeping the most recent 5 years of data
mydata <- mydata[which(mydata$month_id > (max(mydata$month_id)-60)),]

###########################
# pred is the matrix that holds different values that correspond to different
# thresholds for predicting 0 and 1 (everything else is simply rounded)
# cuts is ordinal, values greater than 0 and less than 1
# OR cuts is NA and everything < 1 is 0, else is rounded
# OR cuts is -1, everything < 0 is 0, else is left alone
cuts <- -1
pred <- buildpred(cuts=cuts, df=mydata)

# check to see these look reasonable
peek <- cbind(pred, mydata$pred_ged_sb)

#####
# submit 1000 samples... 100 for parameterization
n <- 100

# for tweedie, mu must be positive, add a smidge
smidge <- .00001

##########################################
### this is for grid-specific

thetamat <- c(.1, .5, 1, 2, 3, 4, 5)
tweediepower <- c(1, 1.25, 1.5, 1.75, 2)
tweediemat <- expand.grid(thetamat, tweediepower)

set.seed(4422)
u <- as.character(unique(mydata$priogrid_gid))

# either t or nb (t for tweedie nb for negative binomial)
mydist <- "t"

if(mydist=='t') {# finalscores will be same row size as pred
  finalscores <- matrix(data=0, nrow=nrow(pred), ncol=4)
  finalscores <- as.data.frame(finalscores)
  fsi <- 1
} else {
  # finalscores will be same row size as pred
  finalscores <- matrix(data=0, nrow=0, ncol=3)
}

# takes about 87 minutes to run
for(gi in 1:length(u)) {
  gdata <- mydata[which(mydata$priogrid_gid==u[gi]),]
  gpred <- buildpred(cuts=cuts, df=gdata)
#  if(mydist=="t") {
    scores <- matrix(data=0, nrow=nrow(gpred), ncol=(nrow(tweediemat)*ncol(gpred)))
 #   }
 # if(mydist=="nb") {
#    scores <- matrix(data=0, nrow=nrow(gpred), ncol=(length(thetamat)*ncol(gpred)))
#  }
    for(i in 1:nrow(gpred)) {
    scoreI <- 1
  
    for(ii in 1:ncol(gpred)) {
    
      # only do these one at a time
    # negative binomial
#      if(mydist=='nb') {
#    for(j in 1:length(thetamat)) {
   #   mytheta <- pred[i,ii]/thetamat[j]
  #    if(mytheta==0) {mytheta<-1}
  #    draws <- rnegbin(n, mu=pred[i,ii], theta=mytheta)
 #     draws <- rnegbin(n, mu=gpred[i,ii], theta=thetamat[j])
#      scores[i,scoreI] <- crps_sample(y=gdata$pred_ged_sb[i], dat=draws)
#      scoreI <- scoreI+1
#    }
      #}
  #    if(mydist=='t') {
      # tweedie
      for(j in 1:nrow(tweediemat)) {
        draws <- round(rtweedie(n=n, power=tweediemat[j,2], mu=gpred[i,ii]+smidge, phi=tweediemat[j,1]), digits=0)
        scores[i,scoreI] <- crps_sample(y=gdata$pred_ged_sb[i], dat=draws)
        scoreI <- scoreI+1
      }
      #}
    }
    }
  sums <- apply(scores, MARGIN=2, FUN=sum)
#  if(mydist=='t') {
#  aa<-Sys.time()
    subscores <- cbind(u[gi], scores[,which.min(sums)], tweediemat[which.min(sums),])
#bb<-Sys.time()-aa
    #  }
#  if(mydist=='nb') {
#    subscores <- cbind(u[gi], scores[,which.min(sums)], thetamat[which.min(sums)])
#  }
#  finalscores <- rbind(finalscores, subscores)
  nss <- nrow(subscores)
  finalscores[fsi:(nss+fsi-1),] <- subscores
  fsi<-fsi+nss
}

#table(finalscores[,2])
#table(finalscores[,3])
#sum(finalscores[,1])

# after this you want to save the distribution's parameters
# so save finalscores as a data.frame?

out <- as.data.frame(finalscores)
if(mydist=="nb") {
  colnames(out) <- c("priogrid_gid", "crps_sum", "theta")
} else {
  colnames(out) <- c("priogrid_gid", "crps_sum", "theta", "power")
}

fn <- paste("/Users/vjdorazio/Desktop/github/forecasting-fast/data/forecasts/params/", mydist, '_', fp, sep='')
write_parquet(out, fn)

#} # end the for loop that writes the grid specific parameters
