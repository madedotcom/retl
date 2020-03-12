context("BigQuery Dataset")

test_that("Check that datasets can be checked", {
  res <- bqDatasetExists()
  expect_true(res, label = "test dataset exists")
  res <- bqDatasetExists("x", project = "test-dummy-project-name")
  expect_true(!res, label = "x dataset does not exist")
})


test_that("Check that dataset can be created", {
  ds <- "ds_retl_to_delete"
  bqCreateDataset(ds)
  res <- bqDatasetExists(ds)
  expect_true(res)

  bqDeleteDataset(ds)
  res <- bqDatasetExists(ds)
  expect_false(res)
})

test_that("Dataset schema is copied", {
  table.name <- "sample_data"
  table.name.partitioned <- "sample_data_partitioned"

  ds.from.name <- "ds_copy_from"
  bqDeleteTable(
    table = table.name,
    dataset = ds.from.name
  )
  bqDeleteTable(
    table = table.name.partitioned,
    dataset = ds.from.name
  )
  bqDeleteDataset(ds.from.name)
  ds.from <- bqCreateDataset(ds.from.name)
  bqInitiateTable(
    table.name,
    dataset = ds.from.name,
    schema.file = "bq-table-schema.json"
  )

  bqInitiateTable(
    table.name.partitioned,
    dataset = ds.from.name,
    schema.file = "bq-table-schema.json",
    partition = TRUE
  )

  ds.to.name <- "ds_to"
  bqDeleteTable(
    table = table.name,
    dataset = ds.to.name
  )
  bqDeleteTable(
    table = table.name.partitioned,
    dataset = ds.to.name
  )
  bqDeleteDataset(ds.to.name)
  ds.to <- bqCreateDataset(ds.to.name)

  bqCopyDatasetSchema(ds.from, ds.to)

  expect_true(bqTableExists(table.name, ds.to.name))
  expect_true(bqTableExists(table.name.partitioned, ds.to.name))
  meta <- bq_table_meta(
    bq_table(
      table = table.name.partitioned,
      dataset = ds.to.name,
      project = ds.to$project
    )
  )
  expect_equal(meta$timePartitioning, list(type = "DAY"))
})
