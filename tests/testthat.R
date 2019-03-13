suppressPackageStartupMessages({
  library(testthat)
  library(devtools)
  library(mockery)
  library(retl)
})

# .Renviron:
# BIGQUERY_TEST_PROJECT
# BIGQUERY_ACCESS_TOKEN_PATH
# BIGQUERY_DATASET
# The access token file needs to be in the testthat folder
# devtools::install_github("jimhester/lintr")
test_check("retl")
