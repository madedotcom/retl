library(bigrquery)
library(mockery)
context("BigQuery query functions.")

test_that("Correct sql is executed", {
  skip_on_travis()

  res <- bqExecuteSql("SELECT %1$s AS value", "'TEST'")
  expect_equal(res$value, "TEST")

  res <- bqExecuteFile("sample_query.sql", 100)
  expect_equal(res$value, 100)
})


test_that("Correct sql statement is read", {
  res <- readSql("sample_query.sql", 500)
  expected <- "SELECT 500 AS value"
  expect_equal(
    res,
    expected,
    label = "Correct query is read with parameter applied"
  )
})
