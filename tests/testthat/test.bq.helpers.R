context("BigQuery Helpers")

test_that("bqCountDuplicates function is correct", {
  test_data = data.frame(
    id = c(1L, 2L, 3L, 4L),
    order_reference = c("101", "101", "202", "202"),
    sku = c("A-UK", "A-UK", "A-UK", "B-ME")
  )

  bqInsertData(
    "bq_count_duplicates",
    test_data,
    "test",
    append = FALSE
  )

  # Zero duplicate rows when grouped by "id"
  res <- bqCountDuplicates(
    table = "bq_count_duplicates",
    dataset = "test",
    keys = "id"
  )
  expect_identical(res, 0L)

  # 1 duplicate row when grouped by "order_reference", "sku"
  res <- bqCountDuplicates(
    table = "bq_count_duplicates",
    dataset = "test",
    keys = c(
      "order_reference",
      "sku"
    )
  )
  expect_identical(res, 1L)

  # Test that function works for empty table
  bqInitiateTable(
    table ="bq_empty_table",
    dataset = "test",
    schema.file = "bq-table-schema.json"
  )

  res <- bqCountDuplicates(
    table = "bq_empty_table",
    dataset = "test",
    keys = "id"
  )
  expect_identical(res, 0L)

})
