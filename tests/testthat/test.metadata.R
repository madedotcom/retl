library(bigrquery)

context("Test Metadata Functions")


test_that("Increment defaults to zero if no records found", {

  with_mock(
    `bigrquery::query_exec` = function(sql, project, default_dataset, page_size) {
      res <- data.frame(job = c(), increment_value = c())
    },
    {
      res.increment <- etlGetIncrement("test")
      expect_identical(res.increment, as.integer(0))
    }
  )
})


test_that("Correct increment is fetched from the database", {

  with_mock(
    `bigrquery::query_exec` = function(sql, project, default_dataset, page_size) {
      res <- data.frame(job = c("test"), increment_value = c(100))
    },
    {
      res.increment <- etlGetIncrement("test")
      expect_identical(res.increment, 100)
    }
  )
})