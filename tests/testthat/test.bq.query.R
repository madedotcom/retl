library(bigrquery)
library(mockery)
context("BigQuery query functions.")

test_that("Correct sql is executed", {
  query_function <- mock(cycle = T)
  with_mock(
    `bigrquery::query_exec` = query_function,
    {
      res <- bqExecuteSql("SELECT %1$s", "TEST")
      expect <- "SELECT TEST"
      expect_args(query_function, 1,  sql = expect, project = "", default_dataset = "", max_pages = Inf)

      res <- bqExecuteFile("sample_query.sql", 100)
      expect <- "SELECT TEST_100"
      expect_args(query_function, 2,  sql = expect, project = "", default_dataset = "", max_pages = Inf)
    }
  )
})
