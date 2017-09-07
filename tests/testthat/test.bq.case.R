context("BigQuery transform to Case testing.")

test_that("Case is calculated correctly", {
  field <- "test_me"
  limits <- c(0, 1)
  names <- c("<= 0", "0 to 1", "1 <")
  res.name <- "res"

  res <- retl::bqVectorToCase(field, limits, names, res.name)

  expect_true(grepl("test_me > 0 AND test_me <= 1", res), "Mid restrain correctly identified")
  expect_true(grepl("END AS res", res), "Resulting name correctly set up")

  field <- "test_me"
  limits <- c(0)
  names <- c("<= 0", "0 <")

  res <- retl::bqVectorToCase(field, limits, names, res.name)

  expect_true(grepl("WHEN \\(test_me <= 0\\) THEN '<= 0' WHEN \\(test_me > 0\\) THEN '0 <' END", res), "Constrains correctly identified")
})
