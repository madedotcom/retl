context("BigQuery transform to Case testing.")

test_that("Case is calculated correctly", {
  field <- "test_me"
  limits <- c(0, 1)
  res.name <- "res"

  res <- retl::bqVectorToCase(field, limits, res.name)

  expect_true(
    grepl("test_me > 0 AND test_me <= 1", res),
    "Mid restrain correctly identified"
  )
  expect_true(
    grepl("END AS res", res),
    "Resulting name correctly set up"
  )

  field <- "test_me"
  limits <- c(0)

  res <- retl::bqVectorToCase(field, limits, res.name)

  expect_true(
    grepl(
      paste0(
        "WHEN \\(test_me <= 0\\) THEN 'A\\) \\(\\-Inf, 0\\]' ",
        "WHEN \\(test_me > 0\\) THEN 'B\\) \\(0, \\Inf\\)' END"
      ),
      res
    ),
    "Constrains correctly identified"
  )
})
