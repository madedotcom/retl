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

  # Param values passed as explicit object should work
  res <- bqExecuteSql(
    sql = "SELECT * FROM (SELECT 'A' AS test) t
           WHERE test IN UNNEST(@tests)",
    tests = bigrquery::bq_param_array("A"),
    use.legacy.sql = FALSE
  )
  expect_equal(res$test, "A")

  # Named values passed to paramaterised query should also work
  res <- bqExecuteSql(
    sql = "SELECT * FROM (SELECT 'A' AS test) t
           WHERE test IN UNNEST(@tests)",
    tests = c(a = "A", b = "B"),
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

  # Dml works with parameters
  job <- bqExecuteDml("UPDATE udpate_test SET speed = @speed WHERE TRUE", speed = 2)
  expect_equal(class(job), "bq_job")

  res <- bqExecuteQuery("SELECT SUM(speed) speed FROM udpate_test")
  expect_equal(res$speed, 100)

})


test_that("Download query output via Storage", {
  res <- bqDownloadQuery("SELECT 1 as field_name")
  expect_equal(res$field.name, 1L)
})

test_that("Table can be created from query", {
  table.test.create <- bq_table(
    project = Sys.getenv("BIGQUERY_PROJECT"),
    dataset = Sys.getenv("BIGQUERY_DATASET"),
    table = "test_create_table_params"
  )


  bqCreateTable(
    sql = "SELECT CAST(@value AS INT64) AS id",
    table = table.test.create$table,
    use.legacy.sql = FALSE,
    schema.file = "bq-table-schema.json",
    value = 1
  )

  res <- bqExecuteQuery(
    query = glue(
      "SELECT * FROM {table}",
      table = table.test.create$table
    )
  )
  expect_equal(res$id, 1L)

  # By default records are appended
  expect_warning(
    bqCreateTable(
      sql = "SELECT CAST(@value AS INT64) AS id",
      table = table.test.create$table,
      use.legacy.sql = FALSE,
      schema.file = "bq-table-schema.json",
      value = 2
    ),
    regexp = "attempting patch"
  )

  res <- bqExecuteQuery(
    query =
      glue(
        "SELECT *
        FROM
          {table}
        ORDER BY
          id",
        table = table.test.create$table
      )
  )
  expect_equal(res$id, c(1L, 2L))

  # Check that metadata is correct and table has two fields per schema definition
  meta <- bq_table_meta(table.test.create)
  expect_equal(meta$schema$fields[[1]]$description, "Unique Identifier")

  # Truncate table and insert new data from query
  expect_warning(
    bqCreateTable(
      sql = "SELECT CAST(@value AS INT64) AS id",
      table = table.test.create$table,
      write.disposition = "WRITE_TRUNCATE",
      use.legacy.sql = FALSE,
      schema.file = "bq-table-schema.json",
      value = 2
    ),
    regexp = "attempting patch"
  )

  # Check that field descriptions persist with `WRITE_TRUNCATE` option
  meta <- bq_table_meta(table.test.create)
  expect_equal(meta$schema$fields[[1]]$description, "Unique Identifier")
  expect_equal(meta$numRows, "1") # check that table was truncated


  # Truncate table and insert new data from query without schema file
  bqCreateTable(
    sql = "SELECT CAST(@value AS INT64) AS id",
    table = table.test.create$table,
    write.disposition = "WRITE_TRUNCATE",
    use.legacy.sql = FALSE,
    value = 2
  )

  # Check that field descriptions persist with `WRITE_TRUNCATE` option
  meta <- bq_table_meta(table.test.create)
  expect_equal(meta$schema$fields[[1]]$description, "Unique Identifier")
  expect_equal(meta$numRows, "1") # check that table was truncated


  # Truncate table and query with wrong schema will fail
  # This confirms that we can change breaking schema changes initiated by sql
  expect_error(
    bqCreateTable(
      sql = "SELECT CAST(@value AS INT64) AS new_field",
      table = table.test.create$table,
      write.disposition = "WRITE_TRUNCATE",
      use.legacy.sql = FALSE,
      value = 2
    ),
    class = "bigrquery_invalid",
    regexp = "new_field is missing"
  )
})
