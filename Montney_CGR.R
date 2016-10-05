#required in main big_decline.R file
library(doSNOW)
library(ffbase)
library(plyr)
library(RColorBrewer)
library(ETLUtils)
library(RODBC)
library(doBy)
library(zoo)
library(snowfall)
#library(rmongodb)
library(mongolite)
library(R.utils)
library(deldir)
#library(geoR)
#library(sp)
#library(RJSONIO)
#library(jsonlite)
#library(rjson)
#library(lubridate)
library(MASS)
library(classInt)



#os <- "Linux"
os <- "Windows"
data_base <- data.frame(vendor="Empty", dsn="Empty", uid="Empty",pwd="Empty")
mongo_data_base <- data.frame(host="Empty",  rsname="Empty", dbname="Empty", timeout=NA_integer_, user="Empty", pwd="Empty", collection="Empty")
#data_base$vendor <- "HPDI"
data_base$vendor <- "PPDM"
max_wells_process <- 10000  # maximum number of wells to analyse in one pass... limitted by RAM
max_wells_dbquery <- 1000	# maximum number of wells you send to a single database query... GLJ's Oracle server limits this to 1000
build_documentation <- FALSE
ResourcePlay <- "Montney"
Welltype <- "Undefined"
iters <- 1000

if(os=="Linux") {
  #export RSTUDIO_WHICH_R=/opt/r/R-3.1.1/bin/R
  CPU_count <- 3
  workdir <- "~/Downloads/Montney_CGR/"
  options(fftempdir="~/Downloads/Montney_CGR/fftemp/")
  data_base$dsn <- "ORAPROD_Pgm_R"
  data_base$uid <- "pgm_r"
  data_base$pwd <- "vE5De9T"
  data_base$uid <- "monolith"
  data_base$pwd <- "RqJCZinDk2I"
  mongo_data_base$host <- "mongodb-prod.corp.gljpc.com:27017"
  mongo_data_base$rsname <- "gljdata_rs01"
  mongo_data_base$dbname <- "Monolith"
  mongo_data_base$user <- "Monolith_Rdr"
  mongo_data_base$pwd <- "jrk6rZ$YVdn"  
  mongo_data_base$user <- "Monolith_Upd"
  mongo_data_base$pwd <- "sdyC0vo6uY"
  mongo_data_base$collection <- "base_declines"
} else {
  CPU_count <- 6
  workdir <- "C:/temp/Montney_CGR/"
  options(fftempdir=paste(workdir, "fftemp", sep=""))
  data_base$dsn <- "ORAWRHS"
  data_base$uid <- "monolith"
  data_base$pwd <- "RqJCZinDk2I"
  mongo_data_base$host <- "mongodb-prod.corp.gljpc.com:27017"
  mongo_data_base$rsname <- "gljdata_rs01"
  mongo_data_base$dbname <- "Monolith"
  mongo_data_base$user <- "Monolith_Rdr"
  mongo_data_base$pwd <- "jrk6rZ$YVdn"  
  mongo_data_base$user <- "Monolith_Upd"
  mongo_data_base$pwd <- "sdyC0vo6uY"
  mongo_data_base$collection <- "base_declines"
}
setwd(workdir)

sapply(list.files(pattern="[.]r$", path=paste(workdir, "R", sep=""), full.names=TRUE), source);

if(build_documentation) {
  write.dcf(
    list(Package = "Montney_CGR",
         Title = "Big Decline", 
         Description = "Tools to calculate decline parameters for bulk well lists.", 
         Version = "0.0", 
         License = "For my eyes only", 
         Author = "Michael Morgan <mmorgan@gljpc.com>", 
         Maintainer = "Michael Morgan <mmorgan@gljpc.com>",
         Imports = "classInt, colorspace, cwhmisc, nloptr, deldir, doBy, doSNOW, ETLUtils, ffbase, geoR, LambertW, lubridate, MASS, mgcv, numDeriv, plyr, quantreg, RColorBrewer, rgdal, rmongodb, RODBC, roxygen2, snowfall, sp, strucchange, TeachingDemos, timeSeries, zoo",
         Suggests = "knitr",
         VignetteBuilder = "knitr"
    ), 
    file = file.path(workdir, "DESCRIPTION")
  )
  roxygen2::roxygenise()
  #devtools::build_vignettes()
  #devtools::document()
  #devtools::build(vignettes=TRUE, manual=TRUE)
  #devtools::build(binary=TRUE, vignettes=TRUE, manual=TRUE)
  #the following creates a sample vignette
  #devtools::use_vignette("my-vignette")
}


if(data_base$vendor =="PPDM") well_query <- "select WELL.X_UWI_DISPLAY, WELL.UWI, WELL.SURFACE_LATITUDE, WELL.SURFACE_LONGITUDE, WELL.X_TD_TVD, WELL.DRILL_TD, WELL.X_ONPROD_DATE, WELL.RIG_RELEASE_DATE, WELL.PROFILE_TYPE, GLJ_PDEN_SUMMARY.PSUM_POOL_NAME, GLJ_PDEN_SUMMARY.PSUM_OPERATOR_NAME from WELL, GLJ_PDEN_SUMMARY where GLJ_PDEN_SUMMARY.PSUM_UWI=WELL.UWI and WELL.X_UWI_DISPLAY in ('"
if(data_base$vendor =="PPDM") prod_query <- "select WELL.X_UWI_DISPLAY, WELL.X_TD_TVD, PDEN_PRODUCTION_MONTH.PROD_DATE, PDEN_PRODUCTION_MONTH.GAS, PDEN_PRODUCTION_MONTH.WATER, PDEN_PRODUCTION_MONTH.OIL_BT, PDEN_PRODUCTION_MONTH.COND, PDEN_PRODUCTION_MONTH.CUM_GAS, PDEN_PRODUCTION_MONTH.CUM_OIL_BT, PDEN_PRODUCTION_MONTH.CUM_WATER, PDEN_PRODUCTION_MONTH.CUM_COND, PDEN_PRODUCTION_MONTH.TOTAL_FLUID, PDEN_PRODUCTION_MONTH.GAS_CAL_DAY, PDEN_PRODUCTION_MONTH.OIL_CAL_DAY, PDEN_PRODUCTION_MONTH.WATER_CAL_DAY, PDEN_PRODUCTION_MONTH.COND_CAL_DAY, PDEN_PRODUCTION_MONTH.TOTAL_FLUID_CAL_DAY, PDEN_PRODUCTION_MONTH.GAS_ACT_DAY, PDEN_PRODUCTION_MONTH.OIL_ACT_DAY, PDEN_PRODUCTION_MONTH.WATER_ACT_DAY, PDEN_PRODUCTION_MONTH.COND_ACT_DAY, PDEN_PRODUCTION_MONTH.TOTAL_FLUID_ACT_DAY from WELL, PDEN_PRODUCTION_MONTH where PDEN_PRODUCTION_MONTH.PDEN_ID=WELL.UWI and WELL.X_UWI_DISPLAY in ('"

if(data_base$vendor =="HPDI") well_query <- "select HPDI_PDEN_DESC.ENTITY_ID, HPDI_PDEN_DESC.API_NO, HPDI_PDEN_DESC.LATITUDE, HPDI_PDEN_DESC.LONGITUDE, HPDI_PDEN_DESC.LOWER_PERF, HPDI_PDEN_DESC.TOTAL_DEPTH, HPDI_PDEN_DESC.FIRST_PROD_DATE, HPDI_PDEN_DESC.COMP_DATE, HPDI_PDEN_DESC.RESERVOIR, HPDI_PDEN_DESC.CURR_OPER_NAME from HPDI_PDEN_DESC where HPDI_PDEN_DESC.ENTITY_ID in ('"
if(data_base$vendor =="HPDI") prod_query <- "select HPDI_PDEN_DESC.ENTITY_ID, HPDI_PDEN_DESC.TOTAL_DEPTH, HPDI_PDEN_PROD.PROD_DATE, HPDI_PDEN_PROD.GAS, HPDI_PDEN_PROD.WTR, HPDI_PDEN_PROD.LIQ from HPDI_PDEN_DESC, HPDI_PDEN_PROD where HPDI_PDEN_PROD.ENTITY_ID=HPDI_PDEN_DESC.ENTITY_ID and HPDI_PDEN_DESC.ENTITY_ID in ('"

# connect to our mongodb
#mongodb <- mongo.create(host=mongo_data_base$host, username=mongo_data_base$user, password=mongo_data_base$pwd, db=mongo_data_base$dbname, timeout=mongo_data_base$timout)



#########################
# Load in the Well List #
#########################

well_list <- read.csv("Well_List_All.csv")[,1]
if(data_base$vendor =="PPDM") well_list <- capitalize(as.character(levels(well_list))[well_list])



#########################################################
# Define colour palettes and number of intervals to use #
#########################################################

num_intervals <- 6
pal_blue <- brewer.pal(3,"Blues") #define bins of colors, blues in this case
pal_red <- brewer.pal(3,"Reds") #define bins of colors, reds in this case
pal_green <- brewer.pal(3,"Greens") #define bins of colors, greens in this case
pal_accent <- brewer.pal(3,"Accent") #define bins of colors, blues in this case



#######################
# Loop for all Groups #
#######################


group <- vector("list", (length(well_list)%/%max_wells_process + if(length(well_list)%%max_wells_process > 0) 1 else 0))
group[[1]] <- well_list[1:min(length(well_list),max_wells_process)]
l=1
while(length(well_list)>max_wells_process*l) {
  group[[l+1]] <- well_list[(1:min((length(well_list)-max_wells_process*l),max_wells_process))+max_wells_process*l]
  l <- l+1
}
rm(l)


#23 groups 6,10,36,42,46
for(k in 1:length(group)){
  
  
  wells <- group[[k]]
  if(data_base$vendor =="HPDI") wells<-sort(wells)
  
  # Query All Wells at once
  temp <- wells[1:min(length(wells),max_wells_dbquery)]
  
  well_data <- read.odbc.ffdf(query=paste(well_query, paste(temp, collapse = "', '"), "') ",sep=""),
                              odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd),VERBOSE=TRUE)
  
  if(data_base$vendor =="PPDM") {
    prod_data <- read.odbc.ffdf(query=paste(prod_query, paste(temp, collapse = "', '"), "') order by PDEN_ID, PROD_DATE",sep=""),
                                odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd),VERBOSE=TRUE)
  }
  
  if(data_base$vendor =="HPDI") {
    prod_data <- read.odbc.ffdf(query=paste(prod_query, paste(temp, collapse = "', '"), "') order by ENTITY_ID, PROD_DATE",sep=""),
                                odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd),VERBOSE=TRUE)
  }
  
  #form_data <- read.odbc.ffdf(query=paste(form_query, paste(temp, collapse = "', '"), "') ",sep=""),
  #                            odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd),VERBOSE=TRUE)
  
  l=1
  while(length(wells)>max_wells_dbquery*l) {
    temp <- wells[(1:min((length(wells)-max_wells_dbquery*l),max_wells_dbquery))+max_wells_dbquery*l]
    well_data<-ffdfappend(well_data,read.odbc.ffdf(query=paste(well_query, paste(temp, collapse = "', '"), "')",sep=""), odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd), VERBOSE=TRUE)[,],adjustvmode=TRUE)
    if(data_base$vendor =="PPDM") {
      temp2 <- read.odbc.ffdf(query=paste(prod_query, paste(temp, collapse = "', '"), "') order by PDEN_ID, PROD_DATE",sep=""), odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd), VERBOSE=TRUE)
    }
    if(data_base$vendor =="HPDI") {
      temp2 <- read.odbc.ffdf(query=paste(prod_query, paste(temp, collapse = "', '"), "') order by ENTITY_ID, PROD_DATE",sep=""), odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd), VERBOSE=TRUE)
    }
    if(dim(temp2)[1]>0) prod_data<-ffdfappend(prod_data,temp2[,],adjustvmode=TRUE)
    #form_data<-ffdfappend(form_data,read.odbc.ffdf(query=paste(form_query, paste(temp, collapse = "', '"), "')",sep=""), odbcConnect.args=list(dsn=data_base$dsn,uid=data_base$uid, pwd=data_base$pwd), VERBOSE=TRUE)[,],adjustvmode=TRUE)
    l <- l+1
  }
  
  
  if(data_base$vendor =="HPDI") {
    well_data <- as.ffdf(renameColumns(well_data[,], from = c("ENTITY_ID", "API_NO", "LATITUDE", "LONGITUDE", "LOWER_PERF", "TOTAL_DEPTH", "FIRST_PROD_DATE", "COMP_DATE", "RESERVOIR", "CURR_OPER_NAME"), to = c("X_UWI_DISPLAY", "UWI", "SURFACE_LATITUDE", "SURFACE_LONGITUDE", "X_TD_TVD", "DRILL_TD", "X_ONPROD_DATE", "RIG_RELEASE_DATE", "PSUM_POOL_NAME", "PSUM_OPERATOR_NAME")))
    prod_data <- as.ffdf(renameColumns(prod_data[,], from = c("ENTITY_ID", "TOTAL_DEPTH", "WTR", "LIQ"), to = c("X_UWI_DISPLAY", "X_TD_TVD", "WATER", "OIL_BT")))
    temp <- as.ffdf(data.frame(COND=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), CUM_COND=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), TOTAL_FLUID=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), COND_CAL_DAY=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), TOTAL_FLUID_CAL_DAY=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), GAS_ACT_DAY=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), OIL_ACT_DAY=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), WATER_ACT_DAY=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), COND_ACT_DAY=rep(NA_real_, length(prod_data$X_UWI_DISPLAY)), TOTAL_FLUID_ACT_DAY=rep(NA_real_, length(prod_data$X_UWI_DISPLAY))))
    prod_data <- as.ffdf(cbind(prod_data[,], temp[,]))
    
    well_data$X_TD_TVD <- well_data$X_TD_TVD*0.3048
    well_data$DRILL_TD <- well_data$DRILL_TD*0.3048
    prod_data$X_TD_TVD <- prod_data$X_TD_TVD*0.3048
    prod_data$GAS <- prod_data$GAS/35.494
    prod_data$WATER <- prod_data$WATER/6.2898
    prod_data$OIL_BT <- prod_data$OIL_BT/6.2898
    
    
    #temp <- as.ffdf(ddply(prod_data[,], .(X_UWI_DISPLAY), HPDI_Cum_Sum, .parallel = TRUE))
    cl <- makeCluster(rep('localhost',CPU_count), type = "SOCK")
    registerDoSNOW(cl)
    clusterExport(cl,c("days_in_month")) 
    temp <- as.ffdf(ddply(prod_data[,], .(X_UWI_DISPLAY), HPDI_Cum_Sum, .parallel = TRUE))
    stopCluster(cl)
    
    #pay particular attention to the order of the wells in the next command... had to add a sort command a few lines up
    prod_data <- as.ffdf(cbind(prod_data[,], temp[,2:length(temp)]))
    
    #temp <- as.ffdf(ddply(prod_data[,], .(X_UWI_DISPLAY), HPDI_Cal_Rates, .parallel = TRUE))
    cl <- makeCluster(rep('localhost',CPU_count), type = "SOCK")
    registerDoSNOW(cl)
    clusterExport(cl,c("days_in_month")) 
    temp <- as.ffdf(ddply(prod_data[,], .(X_UWI_DISPLAY), HPDI_Cal_Rates, .parallel = TRUE))
    stopCluster(cl)
    
    #pay particular attention to the order of the wells in the next command... had to add a sort command a few lines up
    prod_data <- as.ffdf(cbind(prod_data[,], temp[,2:length(temp)]))
  }
  
  if(data_base$vendor =="PPDM") {
    #Exclude all data before 1962 as all we have are cumulate to date for prior time periods
    prod_data <- subset(prod_data,PROD_DATE>strptime("1962-01-01","%Y-%m-%d"))
    levels(well_data$PROFILE_TYPE) <- c(levels(well_data$PROFILE_TYPE), "Horizontal", "Vertical") 
    if(length(well_data$PROFILE_TYPE[well_data$PROFILE_TYPE=="H"][,])>0) well_data$PROFILE_TYPE[well_data$PROFILE_TYPE=="H"][,] <- "Horizontal"
    if(length(well_data$PROFILE_TYPE[well_data$PROFILE_TYPE=="V"][,])>0) well_data$PROFILE_TYPE[well_data$PROFILE_TYPE=="V"][,] <- "Vertical"
    if(length(well_data$PROFILE_TYPE[well_data$PROFILE_TYPE=="D"][,])>0) well_data$PROFILE_TYPE[well_data$PROFILE_TYPE=="D"][,] <- "Vertical"
    well_data$PROFILE_TYPE <- droplevels(well_data$PROFILE_TYPE)
    #suppresWarnigns(expr)
    assign("last.warning",NULL,envir=baseenv())
  }
  
  rm(l,temp,temp2)
  gc()
  
  
  #Update the well list with what was found in the database
  if(data_base$vendor =="PPDM") missing_wells <- setdiff(wells, levels(well_data$X_UWI_DISPLAY))
  if(data_base$vendor =="HPDI") missing_wells <- setdiff(wells, well_data$X_UWI_DISPLAY[,])
  
  
  if (length(missing_wells)>0) {
    write.csv(missing_wells, file = paste("missing_wells_group_", formatC(k, digits=2, big.mark = ",", format = "d"), ".csv", sep="")) 
    if(data_base$vendor =="PPDM")  wells <- intersect(wells, levels(well_data$X_UWI_DISPLAY))
    if(data_base$vendor =="HPDI")  wells <- intersect(wells, well_data$X_UWI_DISPLAY[,])
  } else {
    print(paste("No missing wells in group", formatC(k, digits=2, big.mark = ",", format = "d")))
  }
  
  # Build the results table and fill in the well names and surface locations... use NAs for everything else
  temp <- as.ffdf(
    data.frame(
      UWI=well_data$X_UWI_DISPLAY[,], 
      peak_CGR=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_longterm=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_1=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_2=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_3=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_6=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_12=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_18=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_24=rep(NA_real_, length(well_data$X_UWI_DISPLAY))
    )
  )
  
  temp2 <- as.ffdf(
    data.frame(
      UWI=well_data$X_UWI_DISPLAY[,], 
      peak_CGR=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_longterm=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_6=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_12=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_18=rep(NA_real_, length(well_data$X_UWI_DISPLAY)), 
      CGR_24=rep(NA_real_, length(well_data$X_UWI_DISPLAY))
    )
  )
  
  if(!exists("well_results")) {  
    well_results <- temp
    well_results2 <- temp2
  } else {
    well_results <- ffdfappend(well_results,temp[,],adjustvmode=TRUE)
    well_results2 <- ffdfappend(well_results2,temp2[,],adjustvmode=TRUE)
  }
  
  
  
  
  
  
  #############################
  # Loop for Individual Wells #
  #############################
  
  
  x <- vector("list", length(wells)) # create list
  
  
  # Build a list of the production data required for our decline.  If we are in linux use a multicore algorithm
  cl <- makeCluster(rep('localhost',CPU_count), type = "SOCK")
  registerDoSNOW(cl)
  clusterExport(cl,c("Abandon_Rate_Gas","Abandon_Rate_Oil")) 
  #x <- dlply(subset(prod_data,PROD_DATE<=dates[timestep])[,], .(X_UWI_DISPLAY), Gas_Data, .parallel = TRUE)
  x <- dlply(prod_data[,], .(X_UWI_DISPLAY), Production_Data, .parallel = TRUE)
  stopCluster(cl)
  rm(cl)
  
  
  sfInit(parallel=TRUE, cpus=CPU_count, type="SOCK", socketHosts=rep('localhost',CPU_count) )
  sfExport("CGR_Rates","x")
  temp <- sfClusterApplyLB(1:length(x), function(i) CGR_Rates(x[i], peak_month=0))
  sfStop()
  
  sfInit(parallel=TRUE, cpus=CPU_count, type="SOCK", socketHosts=rep('localhost',CPU_count) )
  sfExport("CGR_Slope","x")
  sfLibrary(MASS)
  temp2 <- sfClusterApplyLB(1:length(x), function(i) CGR_Slope(x[i]))
  sfStop()
  temp2 <- matrix(as.numeric(unlist(temp2)), ncol=6, byrow=TRUE)
  colnames(temp2) <- c("Peak_CGR", "Longterm_CGR", "CGR_First06Months", "CGR_First12Months", "CGR_First18Months", "CGR_First24Months")
  temp2 <- as.data.frame(temp2)
  temp2$UWI <- names(x)
  
  if(data_base$vendor =="PPDM") {
    well_results$peak_CGR[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,1]
    well_results$CGR_longterm[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,2]
    well_results$CGR_1[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,3]
    well_results$CGR_2[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,4]
    well_results$CGR_3[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,5]
    well_results$CGR_6[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,6]
    well_results$CGR_12[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,7]
    well_results$CGR_18[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,8]
    well_results$CGR_24[na.omit(match(as.character(well_results$UWI[,]), levels(prod_data$X_UWI_DISPLAY[,])))] <- matrix(unlist(temp), ncol=9, byrow=TRUE)[,9]
  }
  
  if(data_base$vendor =="HPDI") {
    well_results$peak_CGR[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,1]
    well_results$CGR_longterm[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,2]
    well_results$CGR_1[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,3]
    well_results$CGR_2[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,4]
    well_results$CGR_3[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,5]
    well_results$CGR_6[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,6]
    well_results$CGR_12[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,7]
    well_results$CGR_18[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,8]
    well_results$CGR_24[match(names(x), well_results$UWI[,][][(well_results$Product==products[j])[]])] <- matrix(unlist(temp), ncol=16, byrow=TRUE)[,9]
  }
  
  if(FALSE) {
    CGR.ecdf = ecdf(temp2$CGR_First06Months[!(temp2$CGR_First06Months==0)])
    plot(CGR.ecdf, xlim=c(-.000001,.000001), ylim=c(0,1), xlab = 'Slope in CGR Trend (bbl/MMcf/day)', ylab = 'Percentile', main = 'Empirical Cumluative Distribution\nChange in CGR with Time')
    for(i in 0:10) {
      abline(h = i/10, col="grey")
    }
    abline(v=0, col="grey")
    CGR_06Months <- temp2$CGR_First06Months[!(temp2$CGR_First06Months==0)]
    CGR_12Months <- temp2$CGR_First12Months[!(temp2$CGR_First12Months==0)]
    CGR_18Months <- temp2$CGR_First18Months[!(temp2$CGR_First18Months==0)]
    CGR_24Months <- temp2$CGR_First24Months[!(temp2$CGR_First24Months==0)]
    CGR_06Months.ecdf <- ecdf(CGR_06Months)
    CGR_12Months.ecdf <- ecdf(CGR_12Months)
    CGR_18Months.ecdf <- ecdf(CGR_18Months)
    CGR_24Months.ecdf <- ecdf(CGR_24Months)
    points(CGR_12Months, CGR_12Months.ecdf(CGR_12Months),col="light blue")
    points(CGR_18Months, CGR_18Months.ecdf(CGR_18Months),col="blue")
    points(CGR_24Months, CGR_24Months.ecdf(CGR_24Months),col="dark blue")
    
    CGR.ecdf = ecdf(temp2$CGR_First06Months)
    plot(CGR.ecdf, xlim=c(-.000001,.000001), ylim=c(0,1), xlab = 'Slope in CGR Trend (bbl/MMcf/day)', ylab = 'Percentile', main = 'Empirical Cumluative Distribution\nChange in CGR with Time')
    for(i in 0:10) {
      abline(h = i/10, col="grey")
    }
    abline(v=0, col="grey")
    CGR_06Months <- temp2$CGR_First06Months
    CGR_12Months <- temp2$CGR_First12Months
    CGR_18Months <- temp2$CGR_First18Months
    CGR_24Months <- temp2$CGR_First24Months
    CGR_06Months.ecdf <- ecdf(CGR_06Months)
    CGR_12Months.ecdf <- ecdf(CGR_12Months)
    CGR_18Months.ecdf <- ecdf(CGR_18Months)
    CGR_24Months.ecdf <- ecdf(CGR_24Months)
    points(CGR_12Months, CGR_12Months.ecdf(CGR_12Months),col="light blue")
    points(CGR_18Months, CGR_18Months.ecdf(CGR_18Months),col="blue")
    points(CGR_24Months, CGR_24Months.ecdf(CGR_24Months),col="dark blue")
    
  }
  
  if(FALSE) {
    write.csv(temp2, file = paste(ResourcePlay, "_Group_", k, "_CGR_Slope.csv",sep=""), quote=TRUE)
    
    # Seperate the wells into a series of bins based on the the slop of the CGR and save bins as a series of csv files
    #X <- c(0,2,5,10,25,50,10000)
    temp3 <- subset(temp2, !(is.na(CGR_First18Months)))$CGR_First18Months
    #X <- round(100000000*classIntervals(temp3, n=num_intervals, style="quantile")$brks)/100000000
    X <- c(-400, -15, -5, -1, 1, 5, 15)/100000000
    interval_test_rate <- findInterval(subset(temp2, !(is.na(CGR_First18Months)))$CGR_First18Months, X)
    for(j in 1:length(X)){
      #well_results$UWI[interval_test_rate == j]
      write.csv(
        temp2$UWI[!is.na(temp2$CGR_First18Months)][interval_test_rate == j], 
        file = paste(ResourcePlay, "_Group_", k, " WGR over ",X[j],".csv",sep="")
      ) 
    }
    rm(X)
    
  }
  
  #Clean up the unneeded variables
  rm(temp, temp2, temp3)
  rm(missing_wells)
  gc()
  
  
  if(k==1) {  
    group_results <- merge(well_data, well_results, by.x = "X_UWI_DISPLAY", by.y = "UWI", all.x=FALSE, all.y=FALSE, trace = TRUE)
  } else {
    group_results <- ffdfappend(group_results,merge(well_data, well_results, by.x = "X_UWI_DISPLAY", by.y = "UWI", all.x=FALSE, all.y=FALSE, trace = TRUE)[,],adjustvmode=TRUE)
  }
  
  gc()
  
  
  temp <- subset(well_results,peak_CGR>0)[,]
  temp <- temp[with(temp,order(peak_CGR)),]
  #temp <- subset(temp,log10(peak_CGR)>0.7)
  #temp <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)>0 & (abs(log10(peak_CGR)-log10(CGR_1)) <0.75)))
  
  min_CGR <- 0.01
  
  #plot(temp$peak_CGR,temp$CGR_1,xlim=c(0,200),ylim=c(0,200))
  #plot(CGR_1 ~ peak_CGR, temp, xlim=c(0,200),ylim=c(0,200))  
  temp1 <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)<0.7 & ( log10(CGR_1)>(0.75*log10(peak_CGR)-0.5))))
  temp2 <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)<0.7 & ( log10(CGR_2)>(0.75*log10(peak_CGR)-0.5))))
  temp3 <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)<0.7 & ( log10(CGR_3)>(0.75*log10(peak_CGR)-0.5))))
  temp6 <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)<0.7 & ( log10(CGR_6)>(0.75*log10(peak_CGR)-0.5))))
  temp12 <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)<0.7 & ( log10(CGR_12)>(0.75*log10(peak_CGR)-0.5))))
  temp18 <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)<0.7 & ( log10(CGR_18)>(0.75*log10(peak_CGR)-0.5))))
  temp24 <- subset(temp,log10(peak_CGR)>0.7 | (log10(peak_CGR)<0.7 & ( log10(CGR_24)>(0.75*log10(peak_CGR)-0.5))))
  #loess_fit_1 <- lqs( I(log10(CGR_1)) ~ I(log10(peak_CGR)), na.omit(subset(temp,CGR_1>min_CGR,select=c(peak_CGR,CGR_1))))
  #loess_fit_2 <- lqs( I(log10(CGR_2)) ~ I(log10(peak_CGR)), na.omit(subset(temp,CGR_2>min_CGR,select=c(peak_CGR,CGR_2))))
  #loess_fit_3 <- lqs( I(log10(CGR_3)) ~ I(log10(peak_CGR)), na.omit(subset(temp,CGR_3>min_CGR,select=c(peak_CGR,CGR_3))))
  #loess_fit_6 <- lqs( I(log10(CGR_6)) ~ I(log10(peak_CGR)), na.omit(subset(temp,CGR_6>min_CGR,select=c(peak_CGR,CGR_6))))
  #loess_fit_12 <- lqs( I(log10(CGR_12)) ~ I(log10(peak_CGR)), na.omit(subset(temp,CGR_12>min_CGR,select=c(peak_CGR,CGR_12))))
  #loess_fit_18 <- lqs( I(log10(CGR_18)) ~ I(log10(peak_CGR)), na.omit(subset(temp,CGR_18>min_CGR,select=c(peak_CGR,CGR_18))))
  #loess_fit_24 <- lqs( I(log10(CGR_24)) ~ I(log10(peak_CGR)), na.omit(subset(temp,CGR_24>min_CGR,select=c(peak_CGR,CGR_24))))
  loess_fit_1 <- loess(log10(CGR_1) ~ log10(peak_CGR), na.omit(subset(temp1,CGR_1>min_CGR,select=c(peak_CGR,CGR_1))), span=0.33, family="gaussian",weights=1/peak_CGR)
  loess_fit_2 <- loess(log10(CGR_2) ~ log10(peak_CGR), na.omit(subset(temp2,CGR_2>min_CGR,select=c(peak_CGR,CGR_2))), span=0.33, family="gaussian",weights=1/peak_CGR)
  loess_fit_3 <- loess(log10(CGR_3) ~ log10(peak_CGR), na.omit(subset(temp3,CGR_3>min_CGR,select=c(peak_CGR,CGR_3))), span=0.33, family="gaussian",weights=1/peak_CGR)
  loess_fit_6 <- loess(log10(CGR_6) ~ log10(peak_CGR), na.omit(subset(temp6,CGR_6>min_CGR,select=c(peak_CGR,CGR_6))), span=0.33, family="gaussian",weights=1/peak_CGR)
  loess_fit_12 <- loess(log10(CGR_12) ~ log10(peak_CGR), na.omit(subset(temp12,CGR_12>min_CGR,select=c(peak_CGR,CGR_12))), span=0.33, family="gaussian",weights=1/peak_CGR)
  loess_fit_18 <- loess(log10(CGR_18) ~ log10(peak_CGR), na.omit(subset(temp18,CGR_18>min_CGR,select=c(peak_CGR,CGR_18))), span=0.33, family="gaussian",weights=1/peak_CGR)
  loess_fit_24 <- loess(log10(CGR_24) ~ log10(peak_CGR), na.omit(subset(temp24,CGR_24>min_CGR,select=c(peak_CGR,CGR_24))), span=0.33, family="symmetric",weights=1/peak_CGR)
  
  
  plot(log10(CGR_1) ~ log10(peak_CGR), temp, xaxt="n", yaxt="n", xlim=c(-2,5),ylim=c(-2,5),xlab="Peak CGR (bbl/MMcf)",ylab="CGR after 1 Month",main="Montney CGRs After 1 Month")
  #lines(log10(temp$peak_CGR[!is.na(temp$CGR_1) & (temp$CGR_1>min_CGR)]), predict(loess_fit_1,subset(temp, !is.na(temp$CGR_1) & (temp$CGR_1>min_CGR))), col = "blue")
  lines(log10(temp$peak_CGR[!is.na(temp$CGR_1) & (temp$CGR_1>min_CGR)]), predict(loess_fit_1,subset(temp, !is.na(temp$CGR_1) & (temp$CGR_1>min_CGR))), col = "black")
  abline(a=0,b=1,col="red")
  axis(1,at=(-2:5),labels=expression(10^{-2}, 10^{-1}, 10^{0}, 10^{1}, 10^{2}, 10^{3}, 10^{4}, 10^{5}))
  axis(2,at=(-2:5),labels=expression(10^{-2}, 10^{-1}, 10^{0}, 10^{1}, 10^{2}, 10^{3}, 10^{4}, 10^{5}))
  legend(-2,5, c("Raw Data", "Constant CGR", "Best Fit"), pch=c(1,27,27), lty=c(0,1,1), col=c("Black","Red","Black"))
  
  plot(log10(CGR_6) ~ log10(peak_CGR), temp, xaxt="n", yaxt="n", xlim=c(-2,5),ylim=c(-2,5),xlab="Peak CGR (bbl/MMcf)",ylab="CGR after 6 Months",main="Montney CGRs After 6 Months")
  lines(log10(temp$peak_CGR[!is.na(temp$CGR_6) & (temp$CGR_6>min_CGR)]), predict(loess_fit_1,subset(temp, !is.na(temp$CGR_6) & (temp$CGR_6>min_CGR))), col = "light blue")
  abline(a=0,b=1,col="red")
  axis(1,at=(-2:5),labels=expression(10^{-2}, 10^{-1}, 10^{0}, 10^{1}, 10^{2}, 10^{3}, 10^{4}, 10^{5}))
  axis(2,at=(-2:5),labels=expression(10^{-2}, 10^{-1}, 10^{0}, 10^{1}, 10^{2}, 10^{3}, 10^{4}, 10^{5}))
  legend(-2,5, c("Raw Data", "Constant CGR", "Best Fit"), pch=c(1,27,27), lty=c(0,1,1), col=c("Black","Red","Light Blue"))
  
  plot(log10(CGR_18) ~ log10(peak_CGR), temp, xaxt="n", yaxt="n", xlim=c(-2,5),ylim=c(-2,5),xlab="Peak CGR (bbl/MMcf)",ylab="CGR after 18 Months",main="Montney CGRs After 18 Months")
  lines(log10(temp$peak_CGR[!is.na(temp$CGR_18) & (temp$CGR_18>min_CGR)]), predict(loess_fit_1,subset(temp, !is.na(temp$CGR_18) & (temp$CGR_18>min_CGR))), col = "green")
  abline(a=0,b=1,col="red")
  axis(1,at=(-2:5),labels=expression(10^{-2}, 10^{-1}, 10^{0}, 10^{1}, 10^{2}, 10^{3}, 10^{4}, 10^{5}))
  axis(2,at=(-2:5),labels=expression(10^{-2}, 10^{-1}, 10^{0}, 10^{1}, 10^{2}, 10^{3}, 10^{4}, 10^{5}))
  legend(-2,5, c("Raw Data", "Constant CGR", "Best Fit"), pch=c(1,27,27), lty=c(0,1,1), col=c("Black","Red","Green"))
  
  rm(temp1,temp2,temp3,temp6,temp12,temp18,temp24)
  
  
  plot(temp$peak_CGR[!is.na(temp$CGR_1) & (temp$CGR_1>min_CGR)], 10^(predict(loess_fit_1,subset(temp, !is.na(temp$CGR_1) & (temp$CGR_1>min_CGR))))/temp$peak_CGR[!is.na(temp$CGR_1) & (temp$CGR_1>min_CGR)], xlim=c(0,200),ylim=c(0,1.0), main="Month CGR Trends", xlab="Peak CGR (bbl/MMcf)",ylab="Ratio of CGR to Peak CGR")
  points(temp$peak_CGR[!is.na(temp$CGR_2) & (temp$CGR_2>min_CGR)], 10^(predict(loess_fit_2,subset(temp, !is.na(temp$CGR_2) & (temp$CGR_2>min_CGR))))/temp$peak_CGR[!is.na(temp$CGR_2) & (temp$CGR_2>min_CGR)],col="dark blue")  
  points(temp$peak_CGR[!is.na(temp$CGR_3) & (temp$CGR_3>min_CGR)], 10^(predict(loess_fit_3,subset(temp, !is.na(temp$CGR_3) & (temp$CGR_3>min_CGR))))/temp$peak_CGR[!is.na(temp$CGR_3) & (temp$CGR_3>min_CGR)],col="blue")   
  points(temp$peak_CGR[!is.na(temp$CGR_6) & (temp$CGR_6>min_CGR)], 10^(predict(loess_fit_6,subset(temp, !is.na(temp$CGR_6) & (temp$CGR_6>min_CGR))))/temp$peak_CGR[!is.na(temp$CGR_6) & (temp$CGR_6>min_CGR)],col="light blue")  
  points(temp$peak_CGR[!is.na(temp$CGR_12) & (temp$CGR_12>min_CGR)], 10^(predict(loess_fit_12,subset(temp, !is.na(temp$CGR_12) & (temp$CGR_12>min_CGR))))/temp$peak_CGR[!is.na(temp$CGR_12) & (temp$CGR_12>min_CGR)],col="light green")  
  points(temp$peak_CGR[!is.na(temp$CGR_18) & (temp$CGR_18>min_CGR)], 10^(predict(loess_fit_18,subset(temp, !is.na(temp$CGR_18) & (temp$CGR_18>min_CGR))))/temp$peak_CGR[!is.na(temp$CGR_18) & (temp$CGR_18>min_CGR)],col="green")   
  points(temp$peak_CGR[!is.na(temp$CGR_24) & (temp$CGR_24>min_CGR)], 10^(predict(loess_fit_24,subset(temp, !is.na(temp$CGR_24) & (temp$CGR_24>min_CGR))))/temp$peak_CGR[!is.na(temp$CGR_24) & (temp$CGR_24>min_CGR)],col="dark green")  
  legend(150,1.0, c("1 Month", "2 Months", "3 Months", "6 Months", "12 Months", "18 Months", "24 Months"),col=c("Black","Dark Blue", "Blue", "Light Blue", "Light Green", "Green", "Dark Green"), lty=rep(1,6))
  
  
  
  #Let's try a new algorithm, one that tries to fit a decline to the CGR.  Speficically, let's try fitting a line to a plot of log(CGR) vs Gp
  if(FALSE) {
    for(i in 1:length(x)) {
      i<-0
      i <- 1+i
      CGR_x <- x[[i]]$Qg
      CGR_y <- log((x[[i]]$qo+x[[i]]$qc)/x[[i]]$qg)
      CGR_x <- CGR_x[is.finite(CGR_y)]
      CGR_y <- CGR_y[is.finite(CGR_y)]
      plot(CGR_x, CGR_y)
      CGR_decline <- lqs(CGR_x, CGR_y, method="lms")
      abline(CGR_decline)
    }
  }
  
  
}	# end of the for loop that processes each group








write.csv(group_results, file=paste("Group Results.csv",sep=""),row.names=FALSE)
write.csv(group_results, file=paste("Group Results no NA.csv",sep=""), na="",row.names=FALSE)
#plot(group_results$peak_CGR[,][!is.na(group_results$EUR_exp[,]) & !is.na(group_results$peak_CGR[,])], group_results$EUR_exp[!is.na(group_results$EUR_exp[,]) & !is.na(group_results$peak_CGR[,])],log="xy")
#odbcCloseAll()
#rm(num_intervals)




