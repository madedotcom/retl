library(bigrquery)
library(mockery)

context("BigQuery Dataset")

bq_dataset_exists_mock <- mock(TRUE, FALSE)

test_that("Check that datasets can be checked", {
    res <- bqDatasetExists()
    expect_true(res, label = "test dataset exists")
    res <- bqDatasetExists("x", project = "test-dummy-project-name")
    expect_true(!res, label = "x dataset does not exist")
})

