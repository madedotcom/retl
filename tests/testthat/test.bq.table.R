library(bigrquery)
library(mockery)

context("BigQuery Table")

test_that("table is created from schema file", {
    res <- bqInitiateTable(
      table = "test_table",
      schema.file = "bq-table-schema.json",
      partition = TRUE
    )
    print(res)
    expected.schema <- list(
      list(name = "id",
           type = "INTEGER"),
      list(name = "first_name",
           type = "STRING")
    )
    expected.partition = TRUE
})

