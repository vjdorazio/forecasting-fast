# views3_dub_hpc.R
# reads in: (1) point forecasts and (2) grid-specific parameterizations
# takes 1000 draws for the forecast distribution
# writes the views-formatted output

rm(list=ls())

library(arrow)
library(tweedie)

setwd("/Users/vjdorazio/Desktop/github/forecasting-fast")

# source utils script
source("views_utils.R")

dir <- "/Users/vjdorazio/Desktop/github/forecasting-fast/data/forecasts/"
ff <- list.files(dir)
ff <- unique(grep(paste(".parquet",collapse="|"), ff, value=TRUE))

fd <- paste("params/t_", ff, sep='')

n <- 1000 # draws for views

d <- nchar("_000.parchet")

smidge <- .00001

set.seed(4422)

#    print(i)
i <- 1 # removing the old for loop
  fn <- paste(dir, ff[i], sep='')
  
  # read forecast data and code < 0 to 0
  mydata <- read_parquet(fn)
  mydata$predict[which(mydata$predict<=0)] <- smidge
  
  outdata <- mydata[,c("pred_month_id", "priogrid_gid", "predict")]
  outdata$draw <- list(rep(-1, 1000))
  
  # read tweedie parameter data
  fp <- paste(dir, fd[i], sep='')
  fp <- substr(fp, 1, nchar(fp)-d)
  fp <- paste(fp, ".parquet", sep='')
  myparamsdata <- read_parquet(fp)
 
#  aa<-Sys.time() # about 13 minutes
  for(ii in 1:nrow(mydata)) {
    myparams <- myparamsdata[which(myparamsdata$priogrid_gid==mydata$priogrid_gid[ii]),]
    draws <- round(rtweedie(n=n, power=myparams$power[1], mu=mydata$predict[ii], phi=myparams$theta[1]), digits=0)
    outdata$draw[ii] <- list(draws)
  }
 # bb <- Sys.time()-aa
  
  # write outdata
  colnames(outdata) <- c("month_id", "priogrid_gid", "outcome", "draw")
  fout <- "/Users/vjdorazio/Desktop/github/forecasting-fast/data/draw/draw"
  xt <- substr(ff[i], nchar(ff[i])-d+1, nchar(ff[i]))
  
  # do i have to rewrite month_id here? what is views expecting?
  fout <- paste(fout, xt, sep='')
  write_parquet(outdata, fout)






