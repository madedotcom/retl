context("BigQuery Table")

test_that("table is created from schema file", {
  res <- bqInitiateTable(
    table = "test_table",
    schema.file = "bq-table-schema.json",
    partition = TRUE
  )

  expected.schema <- list(
    list(
      name = "id",
      type = "INTEGER"
    ),
    list(
      name = "first_name",
      type = "STRING"
    )
  )
  expected.partition <- TRUE

  expect_s3_class(res, "bq_table")
  meta <- bq_table_meta(res)
  expect_equal(meta$timePartitioning$type, "DAY")
})

test_that("correct schema is generated for a given dataset", {
  schema <- bqExtractSchema(cars)
  res <- fromJSON(schema)
  expect_equal(res$name, c("speed", "dist"))
  expect_equal(res$type, c("FLOAT", "FLOAT"))
  expect_equal(res$mode, c("NULLABLE", "NULLABLE"))
})

test_that("Correct extension is assigned to a given file format", {
  res <- extensionFromFormat("CSV", "GZIP")
  expect <- "csv.gz"
  expect_equal(res, expect)
})

test_that("Table can be copied", {
  expect_true(!bqTableExists("test_table_copy"))
  bqCopyTable("test_table", "test_table_copy")
  expect_true(bqTableExists("test_table_copy"))
})

test_that("Table can be patched", {
  table <- "test_table"
  res <- bqTableSchema(table = table)
  bqPatchTable(table = table, "bq-table-schema-patch.json")
  table.schema <- bqTableSchema(table = table)

  expect_equal(length(table.schema), 3, label = "Field was added to the table")

  expect_error(
    bqPatchTable(table = table, "bq-table-schema-cleanup.json"),
    regexp = "removed",
    label = "Patch fails if field removal is attempted."
  )

})

test_that("Table with cluster can be created", {
  res <- bqInitiateTable(
    table = "test_table_cluster",
    schema.file = "bq-table-schema.json",
    partition = TRUE,
    clustering = list(fields = list("first_name"))
  )

  expect_s3_class(res, "bq_table")
  meta <- bq_table_meta(res)
  expect_equal(meta$clustering$fields[[1]], "first_name")
})
