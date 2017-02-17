context("Testing AWS S3 helper functions")

test_that("Path is calculated correctly", {
  relative.path <- "subfolder/file.csv"
  Sys.setenv(AWS_S3_ROOT = "prod/project/")
  res <- s3GetObjectPath(relative.path)
  expected <- paste0("prod/project/", relative.path)
  expect_identical(res, expected)
})