options(error = function() {
  sink(stderr())
  on.exit(sink(NULL))
  traceback(3, max.lines = 1L)
  if (!interactive()) {
    q(status = 1)
  }
})

renv::activate()
suppressPackageStartupMessages({
  library(googleAuthR)
  library(jsonlite)
  library(withr)
  library(testthat)
  library(mockery)
  library(R.utils)
  library(retl)
})

# .Renviron:
# BIGQUERY_TEST_PROJECT
# BIGQUERY_ACCESS_TOKEN_PATH
# BIGQUERY_DATASET

# check that you have `sodium` package installed for tests to work locally

# devtools::install_github("jimhester/lintr")
test_check("retl")
