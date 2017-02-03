library(bigrquery)
context("BigQuery Helper functions.")

test_that("Correct sql is executed", {
  with_mock(
    `bigrquery::query_exec` = function(sql, project, default_dataset, max_pages) sql,
    {
      res <- bqExecuteSql("SELECT %1$s", "TEST")
      expect <- "SELECT TEST"
      expect_identical(res, expect)

      res <- bqExecuteSql("SELECT %1$s", "TEST")
      expect <- "SELECT TEST"
      expect_identical(res, expect)

      res <- bqExecuteFile("sample_query.sql", 100)
      expect <- "SELECT TEST_100"
      expect_identical(res, expect)
    }
  )
})
