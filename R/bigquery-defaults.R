#' Gets the value from the corresponding environment variable as boolean
#' Determines which flavour of sql should be used by default.
#' @export
#' @param x Sets `BIGQUERY_LEGACY_SQL` variable if set. Otherwise function returns value of the variable.
bqUseLegacySql <- function(x = NULL) {
  if (is.null(x)) {
    Sys.getenv("BIGQUERY_LEGACY_SQL", unset = "TRUE") == "TRUE"
  }
  else {
    Sys.setenv("BIGQUERY_LEGACY_SQL" = x)
  }
}

bqDefaultProject <- function() {
  Sys.getenv("BIGQUERY_PROJECT")
}

bqDefaultDataset <- function() {
  Sys.getenv("BIGQUERY_DATASET")
}

bqBillingProject <- function() {
  bqDefaultProject()
}

bqStartDate <- function(unset = "2016-01-01") {
  date <- Sys.getenv("BIGQUERY_START_DATE")
  date <- ifelse(nchar(date) > 0, date, unset)
  as.Date(date)
}

bqEndDate <- function(unset = as.character(Sys.Date() - 1)) {
  date <- Sys.getenv("BIGQUERY_END_DATE")
  date <- ifelse(nchar(date) > 0, date, unset)
  as.Date(date)
}
