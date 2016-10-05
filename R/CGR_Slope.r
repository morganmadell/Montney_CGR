#' Calculate the relative increase or decreaste of the Condensate Gas Ratio (bbl/MMcf/month) for various initial production periods
#' 
#' \code{CGR_Slope}
#'
#' @param x list of data to parse
#' @importFrom MASS lqs
#' @export
#' @examples
#' data <- list(SampleWell = data.frame(t=seq(as.POSIXct("2000/1/1"), by = "month", length.out = 100), qg=5*(100:1), Qg=20*(1:100), qfg=rep(25,100), qo=1*(100:1), Qo=4*(1:100), qfo=rep(5,100)))
#' GGR_Rates(data)
CGR_Slope <- function(x){
  #UWI <- names(x[1])
  x<-x[[1]]
  peak_CGR <- NA_real_
  CGR_longterm <- NA_real_
  CGR_6 <- NA_real_
  CGR_12 <- NA_real_
  CGR_18 <- NA_real_
  CGR_24 <- NA_real_
  
  num_months <- length(subset(x,qg>0)$qg)
  if(num_months>2) {
    t <- x$t
    qg <- x$qg
    #Qg <- x$Qg
    qo <- x$qo
    qc <- x$qc
    #Qo <- x$Qo
    #qfg <- max(x$qfg)
    #qfo <- max(x$qfo)
    CGR <- data.frame(Date = t, CGR=1000.0*(qo+qc)/qg, qg=qg)
    
    peak_CGR <- range(subset(CGR,qg>0)$CGR)[2]
    
    #if(length(CGR)>15) {
    #lqs(CGR ~ Date, data=subset(CGR[-c(1:12)],qg>0), method="lms")
    #}
    
    CGR_6 <- try(as.numeric(coefficients(lqs(log(CGR) ~ Date, data=subset(head(CGR,6),(qg>0 & CGR>0)), method="lms"))[2]))
    CGR_12 <- try(as.numeric(coefficients(lqs(log(CGR) ~ Date, data=subset(head(CGR,12),(qg>0 & CGR>0)), method="lms"))[2]))
    CGR_18 <- try(as.numeric(coefficients(lqs(log(CGR) ~ Date, data=subset(head(CGR,18),(qg>0 & CGR>0)), method="lms"))[2]))
    CGR_24 <- try(as.numeric(coefficients(lqs(log(CGR) ~ Date, data=subset(head(CGR,24),(qg>0 & CGR>0)), method="lms"))[2]))
    
    rm(t,qg,qo,qc,CGR)
  }
  
  return(c(peak_CGR, CGR_longterm, CGR_6, CGR_12, CGR_18, CGR_24))
}