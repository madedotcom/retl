suppressPackageStartupMessages({
  library(testthat)
  library(devtools)
  library(mockery)
  library(retl)
})

# .Renviron:
# BIGQUERY_TEST_PROJECT
# BIGQUERY_ACCESS_TOKEN_PATH
# The access token file needs to be in the testthat folder
test_check("retl")
