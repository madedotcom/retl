#' Wrapper for the set_service_token that
#'   uses BIGQUERY_ACCESS_TOKEN_PATH env var
#'   as default value for the secret token location.
#'
#' @description
#' Required environment variables to use BigQuery helper functions:
#'   `BIGQUERY_PROJECT`` - name of the project in BigQuery.
#'   `BIGQUERY_DATASET`` - name of the default dataset in BigQuery.
#'   `BIGQUERY_ACCESS_TOKEN_PATH` - path to the json token file.
bqAuth <- function() {
  if (!bigrquery::bq_has_token()) {
    bigrquery::bq_auth(path = bqTokenFile())
  }
}

bqTokenFile <- function() {
  assert_that(bqTokenFileValid())
  bqTokenFilePath()
}

bqTokenFilePath <- function() {
  Sys.getenv("BIGQUERY_ACCESS_TOKEN_PATH")
}

bqTokenFileValid <- function() {
  token.file <- bqTokenFilePath()
  token.file != "" && file.exists(token.file)
}
