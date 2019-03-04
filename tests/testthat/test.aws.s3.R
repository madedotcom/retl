context("Testing AWS S3 helper functions")

test_that("Path is calculated correctly", {
  relative.path <- "subfolder/file.csv"
  Sys.setenv(AWS_S3_ROOT = "prod/project/")
  res <- s3GetObjectPath(relative.path)
  expected <- paste0("prod/project/", relative.path)
  expect_identical(res, expected)
})

test_that("function is matching the path name", {
  path <- "test/file_name.csv"
  res <- s3GetFile.factory(path)
  expect_equal(res, s3GetFile.csv)

  path <- "test/file_name.json.gz"
  res <- s3GetFile.factory(path)
  expect_equal(res, s3GetFile.json.gz)

  path <- "test/file_name.zip"
  res <- s3GetFile.factory(path)
  expect_equal(res, s3GetFile.zip)

  path <- "test/file_name.csv.gz"
  res <- s3GetFile.factory(path)
  expect_equal(res, s3GetFile.gz)

  path <- "test/file_name.gz"
  res <- s3GetFile.factory(path)
  expect_equal(res, s3GetFile.gz)

  path <- "test/file_name.bad"
  res <-
    expect_error(s3GetFile.factory(path), regexp = "extension.*bad")

  # Put File
  path <- "test/file_name.csv"
  res <- s3PutFile.factory(path)
  expect_equal(res, s3PutFile.csv)

  path <- "test/file_name.json.gz"
  res <- s3PutFile.factory(path)
  expect_equal(res, s3PutFile.json.gz)

  path <- "test/file_name.bad"
  expect_error(s3PutFile.factory(path), regexp = "extension.*bad")

  path <- "test/file_name.csv.gz"
  res <- s3PutFile.factory(path)
  expect_equal(res, s3PutFile.gz)

  path <- "test/file_name.gz"
  res <- s3PutFile.factory(path)
  expect_equal(res, s3PutFile.gz)
})
