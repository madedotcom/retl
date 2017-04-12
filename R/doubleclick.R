#' @import googleAuthR
#' @import jsonlite

library(jsonlite)
library(googleAuthR)

# Authentication requires following environment variables:
# GOOGLE_CLIENT_ID ?
# GOOGLE_CLIENT_SECRET ?
# GOOGLE_REFRESH_TOKEN

# Scope required for authentication:
# https://www.googleapis.com/auth/doubleclicksearch

# Documentation on getting the refresh_token:
# https://developers.google.com/doubleclick-search/v2/prereqs#ds3py

# Set options for Google DC API, possibly should be in .onLoad file.
options(googleAuthR.client_id = Sys.getenv("GOOGLE_CLIENT_ID"),
        googleAuthR.client_secret = Sys.getenv("GOOGLE_CLIENT_SECRET"),
        googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/doubleclicksearch"),
        googleAuthR.httr_oauth_cache = TRUE)


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


dcGetApiUri <- function() {
  uri <- paste0("https://www.googleapis.com/doubleclicksearch/v2/conversion?key=", Sys.getenv("GOOGLE_REFRESH_TOKEN"))
}

dcWriteConversion <-gar_api_generator(dcGetApiUri(), http_header = "POST")

#' Writes custom metrics to DoubleClick Floodlight
#' requires gar_auth()
#'
#' @param clickId gclid unique identifier of the google click
#' @param conversionId unique identifier of the conversion
#' @param metrics named vector of custom metrics
dcWriteCustomMetics <- function(clickId, conversionId, metrics) {
  ts <- paste0(as.integer(as.POSIXct( Sys.time() )) * 1000)
  body <- dcPredictionBody(clickId, conversionId, datetime = ts, metrics)
  res <- dcWriteConversion(the_body = body)
}
