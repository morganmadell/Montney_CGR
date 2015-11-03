#' Returns a data frame with just the variables we need to perform a gas decline.
#' 
#' \code{Gas_Data}
#'
#' @param df data frame to parse
#' @export
#' @examples
#' data <- data.frame(PROD_DATE=50,GAS_CAL_DAY=50,CUM_GAS=200,OIL_CAL_DAY=50,CUM_OIL_BT=200,X_TD_TVD=200)
#' Production_Data(data)
Production_Data <- function(df) {
  #df <- df[with(df, order(PROD_DATE)),]
  data.frame(
    t=df$PROD_DATE, 
    qg=df$GAS_CAL_DAY*35.494, 
    qo=df$OIL_CAL_DAY*6.2898, 
    qc=df$COND_CAL_DAY*6.2898, 
    Qg=df$CUM_GAS*35.494, 
    Qo=df$CUM_OIL_BT*6.2898, 
    Qc=df$CUM_COND*6.2898, 
    qfg=rep(Abandon_Rate_Gas(df$X_TD_TVD[1]), length(df$PROD_DATE)), 
    qfo=rep(Abandon_Rate_Oil(df$X_TD_TVD[1]), length(df$PROD_DATE))
    )
}
