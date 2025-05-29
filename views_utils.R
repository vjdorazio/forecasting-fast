# views_utils.R
# script that has some helper functions like shifting the DV, sets of variable names, etc.
# script is intended to be sourced

# s is a number 1-12, df must have 3 cols named: ged_sb, month_id, and priogrid_id
# function creates a dataframe with 4 cols: pred_ged_sb, month_id, priogrid_gid, pred_month_id
# returns merge with original
builddv <- function(s, df) {
  out <- df
  df <- df[,c("ged_sb", "month_id", "priogrid_gid")]
  colnames(df)[1] <- "pred_ged_sb"
  df$pred_month_id <- df$month_id
  df$month_id <- df$month_id - 2 - s
  out <- merge(out, df, by=c("priogrid_gid", "month_id"), all.x=TRUE)
  return(out)
}

# v is a vector, will grep and return names of all matches
# v should include any of: _sb, _ns, _os, acled_fatalities, acled_fatalities, acled_battles, acled_remote, acled_civvio, acled_protests, acled_riots, acled_stratdev
# a is all other variables to keep, default is month_id and priogrid_gid
getvars <- function(v, df, a=c("month_id", "priogrid_gid")) {
  n <- colnames(df)
  matches <- unique (grep(paste(v,collapse="|"), n, value=TRUE))
  matches <- c(matches, a)
  return(matches)
}

# function that returns matrix of prediction cutoffs
# assumes that df has a column 'predict'
# assumes cuts is an ordinal array with cuts greaters than 0 and less than 1
# if cuts is NA everything less than 1 is 0 and returns 1 column for preds
# could be adjusted to allow cuts greater than 1 cut down to 1 or 0
buildpred <- function(cuts, df) {
  
  if(any(is.na(cuts))) {
    pred <- matrix(df$predict, nrow=nrow(df), ncol=1)
    pred[which(df$predict < 1),1] <- 0
    pred[,1] <- round(pred[,1], digits=0)
    return(pred)
  }
  
  if(cuts==-1) {
    pred <- matrix(df$predict, nrow=nrow(df), ncol=1)
    pred[which(df$predict < 0),1] <- 0
    return(pred)
  }
  
  # pred is the matrix that holds different values that correspond to different
  # thresholds for predicting 0 and 1 (everything else is simply)
  nc <- length(cuts) + 1
  pred <- matrix(df$predict, nrow=nrow(df), ncol=nc)
  
  for(ci in 1:nc) {
    if(ci==nc) {
      pred[which(df$predict < 1),ci] <- 0
      pred[,ci] <- round(pred[,ci], digits=0)
    } else {
      pred[which(df$predict <= cuts[ci]),ci] <- 0
      pred[which(df$predict > cuts[ci] & df$predict < 1),ci] <- 1
      pred[,ci] <- round(pred[,ci], digits=0)
    }
  }
  return(pred)
}


loadpgm <- function() {
  
  # link to ff dropbox https://www.dropbox.com/scl/fo/rkj4ttawoz9pv6x35r9cq/ABODDO2BpL1U47oytDq9VzM/pgm_level_data?rlkey=44eg0kk4w8yh8tm1f53vvpzps&subfolder_nav_tracking=1&dl=0
  
  d <- read_parquet("data/pgm/pgm_data_121_542.parquet")
  
  countries <- read.csv("data/pgm/countries.csv")
  pgmtocountry <- read.csv("data/pgm/priogrid_gid_to_country_id.csv")
  month_ids <- read.csv("data/pgm/month_ids.csv")
  
  d <- merge(d, pgmtocountry[,c("priogrid_gid", "month_id", "country_id")], by= c("priogrid_gid", "month_id"), all.x=TRUE)
  d <- merge(d, countries, by.x="country_id", by.y="id", all.x=TRUE)
  d <- merge(d, month_ids[,2:4], by="month_id", all.x=TRUE)
}
