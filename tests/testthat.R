suppressPackageStartupMessages({
  library(googleAuthR)
  library(jsonlite)
  library(withr)
  library(testthat)
  library(devtools)
  library(mockery)
  library(R.utils)
  library(retl)
})

# .Renviron:
# BIGQUERY_TEST_PROJECT
# BIGQUERY_ACCESS_TOKEN_PATH
# BIGQUERY_DATASET
# The access token file needs to be in the testthat folder
# devtools::install_github("jimhester/lintr")
test_check("retl")
