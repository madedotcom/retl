context("Test mocking")

test_that("Query can be executed against the data stub with standard SQL", {

  sql <- "SELECT * FROM `my.data`"

  with_mock_bigquery({
      res <- bqExecuteQuery(
        sql,
        use.legacy.sql = FALSE
      )
    },
    data.query.path = "bq-stub-query.sql"
  )

  expected <- data.table(id = c(1L, 2L))
  expect_equal(res, expected)

})
