library(bigrquery)
library(mockery)
library(data.table)

context("BigQuery insert functions")
test_that("Data is inserted correctly without metadata", {


    res <- bqInsertData("test_table_insert_empty", data.table())

    dt.test <- data.table(iris)
    colnames(dt.test) <- conformHeader(colnames(dt.test), separator = "_")
    bqInsertData("test_table_insert_iris", dt.test)
})


test_that("Data is inserted correctly with metadata", {
  sepal_length <- NULL
  dt.test <- data.table(iris)

  colnames(dt.test) <- conformHeader(colnames(dt.test), separator = "_")
  dt.test[, sepal_length := as.integer(sepal_length)]

  Sys.setenv("BIGQUERY_METADATA_DATASET" = "metadata_jenkins")

  bqInsertData(
    table = "test_insert_with_meta",
    data = dt.test,
    job.name = "retl.test",
    increment.field = "sepal_length"
  )
  res <- bqExecuteSql("SELECT COUNT(*) cnt FROM test_insert_with_meta")
  expect_equal(res$cnt, nrow(iris))
})

test_that("table can be created from schema", {

  bqInitiateTable(
    table = "table_from_schema",
    schema.file = "test-schema.json"
  )
  expect_true(bqTableExists("table_from_schema"))
})

test_that("partitioned table can be created and data is added", {
  table.name <- "table_daily_from_schema"
  res <- bqInitiateTable(
    table = table.name,
    schema.file = "test-schema.json",
    partition = TRUE
  )
  res <- bqExecuteSql("SELECT _PARTITIONTIME AS value FROM %1$s", table.name)
  expect_equal(nrow(res), 0L)
  dt <- data.table(count = c(1))
  bqInsertPartition(
    table.name,
    date = as.Date("2015-01-01"),
    data = dt
  )
  dates <- bqExistingPartitionDates(table.name)
  expect_equal(dates, "20150101")

  dt <- data.table(count = c(2, 3))
  bqInsertPartition(
    table.name,
    date = as.Date("2015-01-02"),
    data = dt
  )

  bqInitiateTable(
    "table_partion_transformation",
    schema.file = "test-schema.json",
    partition = TRUE
  )
  Sys.setenv(BIGQUERY_START_DATE = "2014-12-30")
  Sys.setenv(BIGQUERY_END_DATE = "2015-01-04")
  bqTransformPartition(
    table = "table_partion_transformation",
    file = "partition-transform.sql",
    table.name # this will will be second parameter in the sql template
  )
  res <- bqExecuteSql(
    "SELECT SUM(count) as result
    FROM table_partion_transformation")
  expect_equal(res$result, 6 * 2)

  dt <- data.table(count = c(1, 1, 1))
  bqInsertPartition(
    table.name,
    date = as.Date("2015-01-01"),
    data = dt
  )

  res <- bqRefreshPartitionData(
    table = "table_partion_transformation",
    file = "partition-transform.sql",
    table.name,
    priority = "INTERACTIVE"
  )

  res <- bqExecuteSql(
    "SELECT SUM(count) as result
    FROM table_partion_transformation")
  expect_equal(res$result, 8 * 2)

})

test_that("shard tables from several datasets can be tranformed in day partitioned tables", {
  Sys.setenv(BIGQUERY_START_DATE = "2015-01-01")
  Sys.setenv(BIGQUERY_END_DATE = "2015-01-02")
  datasets <- c(a = "ds_retl_test_1", b = "ds_retl_test_2")
  lapply(datasets, function(ds) {
    if (bqDatasetExists(ds)) {
      bqDeleteTable("shard_20150101", ds)
      bqDeleteTable("shard_20150102", ds)
    } else {
      bqCreateDataset(ds)
    }
    bqCreateTable(
      sql = "SELECT 1 AS value",
      table = "shard_20150101",
      dataset = ds
    )
    bqCreateTable(
      sql = "SELECT 2 AS value",
      table = "shard_20150102",
      dataset = ds
    )
  })

  bqInitiateTable(
    "partitioned_shards",
    schema.file = "test-schema.json",
    partition = TRUE
  )
  bqCreatePartitionTable(
    table = "partitioned_shards",
    datasets = datasets,
    sql = "SELECT value AS count FROM %1s.shard_%2$s"
  )
  res <- bqExecuteSql("SELECT COUNT(*) as result FROM partitioned_shards")
  expect_equal(res$result, 4)
})

bq_perform_upload_mock <- mock(cycle = T)
wait_for_mock <- mock(cycle = T)
test_that("Metadata logged only if job.name and increment.field params are provided together", {
  with_mock(
    `bigrquery::bq_perform_upload` = bq_perform_upload_mock,
    `bigrquery::wait_for` = wait_for_mock,
    expect_error(
      bqInsertData(
        table = "test_table",
        data.table(iris),
        job.name = "test"),
      "increment\\.field.*required"
    ),
    expect_error(
      bqInsertData(
        table = "test_table",
        data.table(iris),
        increment.field = "test"),
      "job\\.name.*required"
    )
  )
})
