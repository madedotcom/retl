library(bigrquery)
library(mockery)

context("BigQuery Dataset")

bq_dataset_exists_mock <- mock(TRUE, FALSE)

test_that("Check that datasets can be checked", {
    skip_on_travis()
    res <- bqDatasetExists()
    expect_true(res, label = "test dataset exists")
    res <- bqDatasetExists("x", project = "test-dummy-project-name")
    expect_true(!res, label = "x dataset does not exist")
})


test_that("Check that dataset can be created", {
  ds <- "ds_retl_to_delete"
  bqCreateDataset(ds)
  res <- bqDatasetExists(ds)
  expect_true(res)

  bqDeleteDataset(ds)
  res <- bqDatasetExists(ds)
  expect_false(res)
})
