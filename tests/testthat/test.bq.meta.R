context("BigQuery Meta")

test_that("Table description is updated", {
  tbl <- bqInitiateTable(
    table = "test_description_update",
    schema.file = "bq-table-schema.json"
  )
  bqUpdateTableDescription(
    table = "test_description_update",
    description = "test"
  )

  meta <- bigrquery::bq_table_meta(tbl)
  expect_equal(meta$description, "test")
  bqDeleteTable("test_description_update")
})
