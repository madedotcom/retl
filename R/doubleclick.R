#' @import googleAuthR
#' @import assertthat
#' @importFrom assertthat assert_that
NULL

# Authentication requires following environment variables:
# GOOGLE_CLIENT_ID ?
# GOOGLE_CLIENT_SECRET ?
# GOOGLE_REFRESH_TOKEN

# Scope required for authentication:
# https://www.googleapis.com/auth/doubleclicksearch

# Documentation on getting the refresh_token:
# https://developers.google.com/doubleclick-search/v2/prereqs#ds3py

# Set options for Google DC API, possibly should be in .onLoad file.
options(
  googleAuthR.client_id = Sys.getenv("GOOGLE_CLIENT_ID"),
  googleAuthR.client_secret = Sys.getenv("GOOGLE_CLIENT_SECRET"),
  googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/doubleclicksearch"),
  googleAuthR.httr_oauth_cache = TRUE,
  segmentation.type = Sys.getenv("DOUBLECLICK_SEGMENTATION_TYPE"),
  segmentation.name = Sys.getenv("DOUBLECLICK_SEGMENTATION_NAME")
)

dcListConversions <- function(clickId,
                              conversionId,
                              datetime,
                              custom.metrics) {
  assert_that(length(clickId) == 1)

  list(
    clickId = clickId,
    conversionId = conversionId,
    conversionTimestamp = as.character(datetime),
    segmentationType = getOption("segmentation.type"),
    segmentationName = getOption("segmentation.name"),
    customMetric = metricsToList(custom.metrics)
  )
}

#' Creates list for the DoubleClick API call from prediction variables
#'
#' @param clickId gclid for the DoubleClick match for the session
#' @param conversionId unique identifier for the conversion
#' @param datetime POSIX timestamp in milliseconds
#' @param custom.metrics list with named vectors of custom metric values
dcPredictionBody <- function(clickId, conversionId, datetime, custom.metrics) {
  assert_that(
    length(clickId) == length(conversionId),
    length(clickId) == length(datetime),
    length(clickId) == length(custom.metrics)
  )

  conversion.list <- mapply(
    dcListConversions,
    clickId, conversionId, datetime, custom.metrics,
    USE.NAMES = FALSE,
    SIMPLIFY = FALSE
  )

  list(
    kind = "doubleclicksearch#conversionList",
    conversion = conversion.list
  )
}

#' Converts named vector of metrics to a list
#'
#' @param metrics named vector with custom metrics values
metricsToList <- function(metrics) {
  res <- mapply(
    function(value, name) {
      list(
        name = name,
        value = value
      )
    },
    metrics,
    name = names(metrics),
    SIMPLIFY = FALSE
  )
  res <- unname(res)
}

#' Writes custom metrics to DoubleClick Floodlight
#' requires gar_auth()
#' @export
#'
#' @param clickId gclid unique identifier of the google click
#' @param conversionId unique identifier of the conversion
#' @param timestamp POSIX timestamp in seconds as integer
#' @param metrics named vector of custom metrics
#' @return response from the call to DoubleClick search API.
dcWriteCustomMetics <- function(clickId, conversionId, timestamp, metrics) {
  ts <- paste0(as.integer(timestamp) * 1000)
  body <- dcPredictionBody(clickId, conversionId, datetime = ts, metrics)
  dcWriteConversion <- gar_api_generator(
    baseURI = "https://www.googleapis.com/doubleclicksearch/v2/conversion",
    http_header = "POST",
    data_parse_function = function(x) x
  )
  dcWriteConversion(the_body = body)
}
