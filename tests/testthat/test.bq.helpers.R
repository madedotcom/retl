context("BigQuery Helpers")

test_that("bqCountDuplicates function counts duplicate appearances in primary key", {
  test_data = data.frame(
    id = c(1L, 2L, 3L, 4L),
    order_reference = c("101", "101", "202", "202"),
    sku = c("A-UK", "A-UK", "A-UK", "B-ME")
  )

  bqInsertData(
    table = "bq_count_duplicates",
    data = test_data,
    append = FALSE
  )

  # Zero duplicate rows when grouped by "id"
  res <- bqCountDuplicates(
    table = "bq_count_duplicates",
    keys = "id"
  )
  expect_identical(res, 0L)

  # 1 duplicate row when grouped by "order_reference", "sku"
  res <- bqCountDuplicates(
    table = "bq_count_duplicates",
    keys = c(
      "order_reference",
      "sku"
    )
  )
  expect_identical(res, 1L)

})

test_that("bqCountDuplicates returns zero for empty tables", {

  # Test that function works for empty table
  bqInitiateTable(
    table ="bq_empty_table",
    schema.file = "bq-table-schema.json"
  )

  res <- bqCountDuplicates(
    table = "bq_empty_table",
    keys = "id"
  )
  expect_identical(res, 0L)
})
