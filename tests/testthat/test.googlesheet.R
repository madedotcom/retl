context("Google sheet functions")

test_that("Check that sheet can be loaded locally", {
  x <- gsLoadSheet(
    key = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
    tab = "Class Data"
  )
  print(x)
  expect_true(
    nrow(x) == 30,
    label = "30 rows in public sheet"
  )
  expect_true(
    ncol(x) == 6,
    label = "30 rows in public sheet"
  )
})
