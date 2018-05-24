library(bigrquery)
library(mockery)

context("BigQuery Dataset")

bq_dataset_exists_mock <- mock(TRUE, FALSE)
with_mock(
  `bigrquery::bq_dataset_exists` = bq_dataset_exists_mock,
  `retl::bqAuth` = mock(),
  test_that("Check that datasets can be checked", {
      res <- bqDatasetExists("b", project = "test")
      expect_true(res, label = "b dataset exists")
      res <- bqDatasetExists("x", project = "test")
      expect_true(!res, label = "x dataset does not exist")
  })
)
