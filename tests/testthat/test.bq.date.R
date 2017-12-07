context("BigQuery Dates Tables/Partitions.")

test_that("Missing dates function is correct.", {
  Sys.setenv(TZ = "GMT")
  start.date <- as.Date("2015-01-10")
  end.date <- as.Date("2015-01-20")
  # Dates are given in format %Y%m%d.
  existing.dates <- c("20150110", "20150111", "20150112")
  res <- getMissingDates(start.date, end.date, existing.dates)

  expect_identical(length(res), as.integer(8), "Existing dates are exluded.")

})
