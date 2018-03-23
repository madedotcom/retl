library(bigrquery)
library(mockery)

context("BigQuery Dataset")

list_datasets_mock <- function(project) {
  list("a", "b", "c")
}
with_mock(
  `bigrquery::list_datasets` = list_datasets_mock,
  test_that("Check that datasets can be checked", {
      res <- bqDatasetExists("b", project = "test")
      expect_true(res, label = "b dataset exists")
      res <- bqDatasetExists("x", project = "test")
      expect_true(!res, label = "x dataset does not exist")
  })
)
