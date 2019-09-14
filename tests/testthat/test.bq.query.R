context("BigQuery query functions.")

test_that("Correct sql is executed", {
  res <- bqExecuteQuery("SELECT '%1$s' AS value", "TEST")
  expect_equal(res$value, "TEST")

  res <- bqExecuteFile("sample_query.sql", 100)
  expect_equal(res$value, 100)
})

test_that("Parameters work", {
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

test_that("Parameter can be used with a vector", {
  res <- bqExecuteSql(
    sql = "SELECT * FROM (SELECT 'A' AS test) t
           WHERE test IN UNNEST(@tests)",
    tests = c("A", "B"),
    use.legacy.sql = FALSE
  )
  expect_equal(res$test, "A")

  res <- bqExecuteSql(
    sql = "SELECT * FROM (SELECT 'A' AS test) t
           WHERE test IN UNNEST(@tests)",
    tests = bigrquery::bq_param_array("A"),
    use.legacy.sql = FALSE
  )
  expect_equal(res$test, "A")
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

test_that("Can execute DML query", {
  bqInsertData(table = "udpate_test", data = cars)
  job <- bqExecuteDml("UPDATE udpate_test SET speed = 1 WHERE TRUE")
  expect_equal(class(job), "bq_job")

  res <- bqExecuteQuery("SELECT SUM(speed) speed FROM udpate_test")
  expect_equal(res$speed, 50)
})
