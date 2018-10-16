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

test_that("Query can be combined by supporting function", {
  sql <- bqCombineQueries(
    sql = list(
      "SELECT 1 a",
      "SELECT 5 a"
    ),
    use.legacy.sql = TRUE
  )

  res <- bqExecuteSql(sql, use.legacy.sql = TRUE)
  expect_equal(res$a, c(1, 5))

  # Same as above works for Standard sql
  sql <- bqCombineQueries(
    sql = list(
      "SELECT 1 a",
      "SELECT 5 a"
    ),
    use.legacy.sql = FALSE
  )

  res <- bqExecuteSql(sql, use.legacy.sql = FALSE)
  expect_equal(res$a, c(1, 5))
})
