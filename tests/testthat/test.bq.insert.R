context("BigQuery insert functions")

test_that("Data is inserted correctly without metadata", {
  skip_on_travis()
  res <- bqInsertData("test_table_insert_empty", data.table())

  dt.test <- data.table(iris)
  bqInsertData("test_table_insert_iris", dt.test)

  # get data back
  iris.bq <- bqExecuteQuery("SELECT * FROM test_table_insert_iris")

  expect_equal(nrow(iris.bq), nrow(dt.test))
  # data from bigquery matches the data we sent, up to:
  # * column names
  # * types
  # * order
  colnames(dt.test) <- tolower(colnames(dt.test))
  iris.bq[, species := as.factor(species)]
  expect_equal(
    iris.bq[order(sepal.length, sepal.width, petal.length, petal.width)],
    dt.test[order(sepal.length, sepal.width, petal.length, petal.width)]
  )
})

test_that("Large dataset is inserted correctly", {
  skip_on_travis()
  res <- bqInsertLargeData("test_table_insert_empty", data.table())

  dt.test <- data.table(iris)
  bqInsertLargeData("test_table_insert_large_iris", dt.test)

  # get data back
  iris.bq <- bqExecuteQuery("SELECT * FROM test_table_insert_large_iris")

  expect_equal(nrow(iris.bq), nrow(dt.test))
  # data from bigquery matches the data we sent, up to:
  # * column names
  # * types
  # * order
  colnames(dt.test) <- tolower(colnames(dt.test))
  iris.bq[, species := as.factor(species)]
  expect_equal(
    iris.bq[order(sepal.length, sepal.width, petal.length, petal.width)],
    dt.test[order(sepal.length, sepal.width, petal.length, petal.width)]
  )
})

test_that("table can be created from schema", {
  skip_on_travis()
  bqInitiateTable(
    table = "table_from_schema",
    schema.file = "test-schema.json"
  )
  expect_true(bqTableExists("table_from_schema"))
})

test_that("partitioned table can be created and data is added", {
  skip_on_travis()
  table.name <- "table_daily_from_schema"
  res <- bqInitiateTable(
    table = table.name,
    schema.file = "test-schema.json",
    partition = TRUE
  )
  res <- bqExecuteSql("SELECT _PARTITIONTIME AS value FROM %1$s", table.name)
  expect_equal(nrow(res), 0L)
  dt <- data.table(count = c(1L))
  bqInsertPartition(
    table.name,
    date = as.Date("2015-01-01"),
    data = dt
  )
  dates <- bqExistingPartitionDates(table.name)
  expect_equal(dates, "20150101")

  dt <- data.table(count = c(2L, 3L))
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
    FROM table_partion_transformation"
  )
  expect_equal(res$result, 6 * 2)

  dt <- data.table(count = c(1L, 1L, 1L))
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
    FROM table_partion_transformation"
  )
  expect_equal(res$result, 8 * 2)
})

context("Create partition table from shards")

test_that("shard tables from several datasets can
          be tranformed in day partitioned tables", {
  skip_on_travis()
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

  # partition is updated with BATCH priority and results don't change
  bqCreatePartitionTable(
    table = "partitioned_shards",
    datasets = datasets,
    sql = "SELECT value AS count FROM `%1s.shard_%2$s",
    priority = "INTERACTIVE",
    use.legacy.sql = FALSE
  )
  res <- bqExecuteSql("SELECT COUNT(*) as result FROM partitioned_shards")
  expect_equal(res$result, 4)

})
