library(mockery)
context("BigQuery Create Table")
auth_mock = mock(cycle = T)
insert_table_mock <- mock()
with_mock(
  insert_table = insert_table_mock,
  bqAuth = auth_mock,
  test_that("Check that schema is passed to insert_table", {

    bqInitiateTable(table = "test_table",
                    schema.file = "bq-table-schema.json",
                    partition = TRUE)

    expected.schema <- list(
      list(name = "id",
           type = "INTEGER"),
      list(name = "first_name",
           type = "STRING")
    )
    expected.partition = TRUE

    expect_args(
      insert_table_mock,
      1,
      project = Sys.getenv("BIGQUERY_PROJECT"),
      dataset = Sys.getenv("BIGQUERY_DATASET"),
      table = "test_table",
      schema = expected.schema,
      partition = expected.partition
    )
  })
)
