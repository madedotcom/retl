library(bigrquery)

context("Test Metadata Functions")

test_that("Increment defaults to zero if no records found", {
  res.increment <- etlGetIncrement("test.dummy.name")
  expect_identical(res.increment, as.integer(0))
})

test_that("Correct increment is saved and fetched from the database", {
  etlLogExecution("test.retl.job", increment.value = 1000L, records = 0L)
  res.increment <- etlGetIncrement("test.retl.job")
  expect_identical(res.increment, 1000L)
})
