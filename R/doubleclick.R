#' @import googleAuthR
#' @import jsonlite

library(jsonlite)
library(googleAuthR)


#' Creates list for the DoubleClick API call from prediction variables
#'
#' @param clickId gclid for the DoubleClick match for the session
#' @param conversionId unique identifier for the conversion
#' @param datetime POSIX timestamp in miliseconds
#' @param custom.metrics name vector of custom metric values
dcPredictionBody <- function(clickId, conversionId, datetime, custom.metrics) {
  body = list(
    kind = "doubleclicksearch#conversionList",
    conversion = list(
      list(
        clickId = clickId,
        conversionId = conversionId,
        conversionTimestamp = as.character(datetime),
        segmentationType = "FLOODLIGHT",
        segmentationName = "ML",
        customMetric = metricsToList(custom.metrics)
      )
    )
  )
}

#' Converts named vector of metrics to a list
#'
#' @param metrics named vector with custom metrics values
metricsToList <- function(metrics) {
  res <- mapply(function(value, name) {
     list(
       name = name,
       value = value
     )
    }, metrics, name = names(metrics), SIMPLIFY = F)
  res <- unname(res)
}


dcWriteConversion <-gar_api_generator("https://www.googleapis.com/doubleclicksearch/v2/conversion", http_header = "POST")


dcWriteCustomMetics <- function(clickId, conversionId, metrics) {

  ts <- paste0(as.integer(as.POSIXct( Sys.time() )) * 1000)
  body <- dcPredictionBody(clickId, conversionId, datetime = ts, metrics)
  print(body)
  gar_auth_service(json_file = "access_token.json")
  res <- dcWriteConversion(the_body = body)
  print(res)
}
