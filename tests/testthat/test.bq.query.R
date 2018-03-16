library(bigrquery)
library(mockery)
context("BigQuery query functions.")
auth_mock = mock(cycle = T)
test_that("Correct sql is executed", {
  query_function <- mock(cycle = T)
  with_mock(
    `bigrquery::query_exec` = query_function,
    `retl::bqAuth` = auth_mock,
    {
      res <- bqExecuteSql("SELECT %1$s", "TEST")
      expect <- "SELECT TEST"
      expect_args(query_function, 1,  sql = expect, project = "", default_dataset = "", max_pages = Inf, TRUE)

      res <- bqExecuteFile("sample_query.sql", 100)
      expect <- "SELECT TEST_100"
      expect_args(query_function, 2,  sql = expect, project = "", default_dataset = "", max_pages = Inf, TRUE)
    }
  )
})


test_that("Correct sql statement is read", {
  res <- readSql("sample_query.sql", 500)
  expected <- "SELECT TEST_500"
  expect_equal(res, expected, label = "Correct query is read with parameter applied")
})
