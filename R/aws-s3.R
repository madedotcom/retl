#' @import utils
#' @import data.table
#' @import aws.s3
#' @import jsonlite

library(data.table)
library(aws.s3)
library(jsonlite)

# To access AWS S3 following environment variables are required:
#
# AWS_S3_BUCKET - defines the name of the bucket in aws s3.
# AWS_S3_ROOT - defines the root path for the project.
#
# Also aws.s3 package required following env variables:
# Sys.setenv("AWS_ACCESS_KEY_ID" = "mykey",
#            "AWS_SECRET_ACCESS_KEY" = "mysecretkey",
#            "AWS_DEFAULT_REGION" = "us-east-1",
#            "AWS_SESSION_TOKEN" = "mytoken")
# See: https://github.com/cloudyr/aws.s3

#' Saves data to a file in AWS S3
#'
#' Convention based wrapper functions that allow to save data in S3
#' @export
#' @param dt data.table to save
#' @param path S3 object path starting after root folder.
#' @param bucket name of the S3 bucket
#' @param root project root path that is appended before the path, e.g. "/prod/"
#' @param na.value the string to use for missing values in the data
#' @return `s3PutFile` TRUE if csv file was saved to S3
s3PutFile <- function(dt, path,
                      bucket = Sys.getenv("AWS_S3_BUCKET"),
                      root = Sys.getenv("AWS_S3_ROOT"),
                      na.value = "") {
  tmp.file <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp.file))

  write.csv(dt, file = tmp.file, row.names = FALSE, fileEncoding = "UTF-8", na = na.value)
  full.path <- paste0(root, path)
  put_object(file = tmp.file, object = full.path, bucket = bucket)
}

#' Loads data from AWS S3 into data.table object
#'
#' Convention based wrapper functions that allow to load
#' data files in S3 into data.table
#'
#' @export
#' @param path is the path to the S3 object
#' @param header flag defines whether file has header
#' @param bucket name of the S3 bucket.
#'     Defautls to value in `AWS_S3_BUCKET` environment variable.
#' @param root project root path that is appended before the path in the argument.
#'     Defaults to value in `AWS_S3_ROOT` environment variable.
#' @return `s3GetFile` gets data from source `.csv` file
s3GetFile <- function(path, header = T,
                      bucket = Sys.getenv("AWS_S3_BUCKET"),
                      root = Sys.getenv("AWS_S3_ROOT")) {
  full.path <- paste0(root, path)
  raw_data <- get_object(full.path, bucket)

  # In case of error, print error message.
  if (!is.raw(raw_data))
    stop(print(raw_data[1:3]))

  data <- iconv(readBin(raw_data, character()), from = "UTF-8", to = "UTF-8")
  dt <- fread(data, header = header, strip.white = F)
  names(dt) <- conformHeader(names(dt))
  invisible(dt)
}

#' Loads data from several files in S3 based on the path prefix
#'
#' @export
#'
#' @note You need make sure that all targeted files have the same header signature:
#'   order of the fields should not matter.
#'
#' @inheritParams s3GetFile
#' @param s3Get.FUN Function that will be used to read data from the individual files.
#' @param fill see data.table::rbindList
#' @return data.table with data combined from files. It will be Null data.table if path
#' did not match any of the files in S3 bucket.
s3GetData <- function(path, header = T,
                      bucket = Sys.getenv("AWS_S3_BUCKET"),
                      root = Sys.getenv("AWS_S3_ROOT"),
                      s3Get.FUN = s3GetFile,
                      fill = T) {

  full.path <- paste0(root, path)
  full.path <- gsub("^/", "", full.path)
  objects <- aws.s3::get_bucket(bucket = bucket, prefix = full.path)
  dt.list <- lapply(objects, function(o) {
    s3Get.FUN(o$Key, bucket = bucket, root = "")
  })

  invisible(rbindlist(dt.list, use.names = T, fill = fill))
}

#' Saves data.table as csv.gz to S3
#'
#' @export
#' @rdname s3PutFile
#' @return `s3PutFile.gz` TRUE if `.gz` (csv archived) file was saved to S3.
s3PutFile.gz <- function(dt, path,
                         bucket = Sys.getenv("AWS_S3_BUCKET"),
                         root = Sys.getenv("AWS_S3_ROOT"),
                         na.value = "") {

  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))
  gz.connection <- gzfile(tmp.file, "w")
  write.csv(dt, file = gz.connection, row.names = FALSE, fileEncoding = "UTF-8", na = na.value)
  close(gz.connection)

  full.path <- paste0(root, path)
  put_object(file = tmp.file, object = full.path, bucket = bucket)
}


#' @export
#' @rdname s3GetFile
#' @return `s3GetFile.gz` loads data from `.gz` files
s3GetFile.gz <- function(path,
                         bucket = Sys.getenv("AWS_S3_BUCKET"),
                         root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))

  save_object(full.path, bucket, file = tmp.file)
  dt <- fread(paste0('zcat < ', tmp.file))
  names(dt) <- conformHeader(names(dt))
  invisible(dt)
}

#' @export
#' @rdname s3PutFile
#' @return `s3PutFile.rds` TRUE if `.rds` file was saved to S3
s3PutFile.rds <- function(dt, path,
                          bucket = Sys.getenv("AWS_S3_BUCKET"),
                          root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  s3saveRDS(dt, bucket = bucket, object = full.path)
}

#' @export
#' @rdname s3GetFile
#' @return `s3GetFile.rds` loads data from `.rds` files
s3GetFile.rds <- function(path,
                          bucket = Sys.getenv("AWS_S3_BUCKET"),
                          root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  s3readRDS(bucket = bucket, object = full.path)
}

#' Gets the path to the object based on the root path setup via
#' environment variable
#'
#' @export
#' @param relative.path defines the path to the object post project root path.
s3GetObjectPath <- function(relative.path) {
  path <- paste0(Sys.getenv("AWS_S3_ROOT"), relative.path)
  return(path)
}

#' @rdname s3GetFile
#' @export
#' @return `s3GetFile.zip` loads data from `.zip` files
s3GetFile.zip <- function(path,
                         bucket = Sys.getenv("AWS_S3_BUCKET"),
                         root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  tmp.file <- tempfile(fileext = ".zip")
  on.exit(unlink(tmp.file))

  save_object(full.path, bucket, file = tmp.file)
  dt <- fread(unzip(tmp.file), fill = TRUE)
  names(dt) <- conformHeader(names(dt))
  invisible(dt)
}

#' @rdname s3PutFile
#' @export
#' @return `s3PutFile.json.gz` TRUE if `.json.gz` (json archived) file was saved to S3
s3PutFile.json.gz <- function(dt, path,
                         bucket = Sys.getenv("AWS_S3_BUCKET"),
                         root = Sys.getenv("AWS_S3_ROOT")) {

  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))
  gz.connection <- gzfile(tmp.file, "w")
  write_json(dt, path = gz.connection)
  close(gz.connection)

  full.path <- paste0(root, path)
  put_object(file = tmp.file, object = full.path, bucket = bucket)
}

#' @rdname s3GetFile
#' @export
#' @return `s3GetFile.json.gz` loads data from `.json.gz` files
s3GetFile.json.gz <- function(path,
                         bucket = Sys.getenv("AWS_S3_BUCKET"),
                         root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))

  save_object(full.path, bucket, file = tmp.file)
  dt <- fromJSON(tmp.file)
  dt <- data.table(dt)
  names(dt) <- conformHeader(names(dt))
  invisible(dt)
}
