#' Calculate the gas reserves using several different exponential and harmonic declines.
#' 
#' \code{Gas_Reserves}
#'
#' @param x list of data to parse
#' @export
#' @examples
#' data <- list(SampleWell = data.frame(t=seq(as.POSIXct("2000/1/1"), by = "month", length.out = 100), qg=5*(100:1), Qg=20*(1:100), qfg=rep(25,100), qo=1*(100:1), Qo=4*(1:100), qfo=rep(5,100)))
#' GGR_Rates(data)
CGR_Rates <- function(x, peak_month=0, min_CGR=0.01){
  UWI <- names(x[1])
  x <- subset(x[[1]],qg>0)
  t <- x$t
  qg <- x$qg
  Qg <- x$Qg
  qo <- x$qo
  qc <- x$qc
  Qo <- x$Qo
  qfg <- max(x$qfg)
  qfo <- max(x$qfo)
  peak_CGR <- NA_real_
  CGR_longterm <- NA_real_
  CGR_j <- rep(NA_real_, 24)
  num_months <- length(qg)
  
  if(num_months>1) {
    CGR <- 1000.0*(qo+qc)/qg
    peak_CGR_index <- which.max(CGR)
    peak_CGR <- CGR[peak_CGR_index]
    if(!(peak_month == 0)) {
      peak_CGR_index <- peak_month
    }
    if(is.na(peak_CGR)) peak_CGR <- min_CGR
    if(peak_CGR<min_CGR) peak_CGR <- min_CGR
    CGR_interval <- min(length(CGR_j),(num_months-peak_CGR_index))
    if(CGR_interval>0) {
      CGR_j[1:CGR_interval] <- CGR[(peak_CGR_index+1):(peak_CGR_index+CGR_interval)]
    }
  }
  
  CGR_1 <- CGR_j[1]
  CGR_2 <- CGR_j[2]
  CGR_3 <- CGR_j[3]
  CGR_6 <- CGR_j[6]
  CGR_12 <- CGR_j[12]
  CGR_18 <- CGR_j[18]
  CGR_24 <- CGR_j[24]
  return(c(peak_CGR, CGR_longterm, CGR_1, CGR_2, CGR_3, CGR_6, CGR_12, CGR_18, CGR_24))
}