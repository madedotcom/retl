library(bigrquery)
library(mockery)
context("BigQuery query functions.")

test_that("Correct sql is executed", {
  skip_on_travis()

  res <- bqExecuteQuery("SELECT '%1$s' AS value", "TEST")
  expect_equal(res$value, "TEST")

  res <- bqExecuteFile("sample_query.sql", 100)
  expect_equal(res$value, 100)
})

test_that("Parameters work", {
  skip_on_travis()
  res <- bqExecuteQuery(
    "SELECT @test AS value",
    test = "TEST",
    use.legacy.sql = FALSE
  )
  expect_equal(res$value, "TEST")

  res <- bqExecuteFile(
    "sample_query_param.sql",
    test = 5L,
    use.legacy.sql = FALSE
  )
  expect_equal(res$value, 5)
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


