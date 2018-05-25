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
