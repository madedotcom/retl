#' Loads data from GoodData
#'
#' @export
#' @import rGoodData
#' @param report.id identifier of the report
gdLoadReport <- function(report.id) {
  definition.id <- getLastDefinition(report.id)
  report.uri <- getReportRawUri(definition.id)
  dt <- getReportData(report.uri)
  return(dt)
}
