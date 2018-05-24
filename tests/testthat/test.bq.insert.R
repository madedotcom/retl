library(bigrquery)
library(mockery)
library(data.table)

context("BigQuery insert functions")
auth_mock = mock(cycle = T)
bq_perform_upload_mock <- mock(cycle = T)
wait_mock <- mock(cycle = T)
test_that("Data is inserted correctly without metadata", {
  with_mock(
    `bigrquery::bq_perform_upload` = bq_perform_upload_mock,
    `bigrquery::wait_for` = wait_mock,
    `retl::bqAuth` = auth_mock,
    {
      res <- bqInsertData("test_table", data.table())
      expect_called(bq_perform_upload_mock, 0)

      res <- bqInsertData("test_table", iris)
      expect_called(bq_perform_upload_mock, 1)
    }
  )
})

bq_perform_upload_mock <- mock(cycle = T)
wait_mock <- mock(cycle = T)
test_that("Data is inserted correctly with metadata", {
  with_mock(
    `bigrquery::bq_perform_upload` = bq_perform_upload_mock,
    `bigrquery::wait_for` = wait_mock,
    `retl::bqAuth` = auth_mock,
    {
      dt.test <- data.table(iris)
      res <- bqInsertData(table = "test_table", dt.test, job.name = "test", increment.field = "Sepal.Width")
      expect_called(bq_perform_upload_mock, 2) # calls add up, so only two calls are attributed to this test
      expect_args(bq_perform_upload_mock, 2,
                  project = "",
                  dataset = "",
                  table = "etl_increments",
                  values = data.table(job = "test",
                                      increment_value = as.integer(max(iris$Sepal.Width)),
                                      records = nrow(iris),
                                      datetime = Sys.time()),
                  write_disposition = "WRITE_APPEND",
                  create_disposition = "CREATE_NEVER"
      )
    }
  )
})

insert_upload_job_mock <- mock(cycle = T)
wait_for_mock <- mock(cycle = T)
test_that("Metadata logged only if job.name and increment.field params are provided together", {
  with_mock(
    `bigrquery::insert_upload_job` = insert_upload_job_mock,
    `bigrquery::wait_for` = wait_for_mock,
    expect_error(bqInsertData(table = "test_table", data.table(iris), job.name = "test"), "increment\\.field.*required"),
    expect_error(bqInsertData(table = "test_table", data.table(iris), increment.field = "test"), "job\\.name.*required")
  )
})
