library(bigrquery)
library(mockery)

context("BigQuery Table")

test_that("table is created from schema file", {
  skip_on_travis()
  res <- bqInitiateTable(
    table = "test_table",
    schema.file = "bq-table-schema.json",
    partition = TRUE
  )

  expected.schema <- list(
    list(name = "id",
         type = "INTEGER"),
    list(name = "first_name",
         type = "STRING")
  )
  expected.partition = TRUE
})

test_that("correct schema is generated for a given dataset", {
  schema <- bqExtractSchema(cars)
  res <- fromJSON(schema)
  expect_equal(res$name, c("speed", "dist"))
  expect_equal(res$type, c("FLOAT", "FLOAT"))
  expect_equal(res$mode, c("NULLABLE", "NULLABLE"))
})
