#' @import utils
#' @import data.table
#' @import aws.s3

library(data.table)
library(aws.s3)

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


#' Saves a data.frame structure to AWS S3 as a csv file
#'
#' @export
#' @param df data.frame to save
#' @param path S3 object path starting after root folder.
#' @param bucket name of the S3 bucket
#' @param root project root path that is appended before the path, e.g. "/prod/"
s3PutFile <- function(df, path,
                       bucket = Sys.getenv("AWS_S3_BUCKET"),
                       root = Sys.getenv("AWS_S3_ROOT")) {
  tmp.file <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp.file))

  write.csv(df, file = tmp.file, row.names = F, fileEncoding = "UTF-8")
  full.path <- paste0(root, path)
  put_object(file = tmp.file, object = full.path, bucket = bucket)
}


#' Loads data from a csv file in AWS S3 to data.table
#'
#' @export
#' @param path is the path to the S3 object
#' @param header flag defines whether file has header
#' @param bucket name of the S3 bucket
#' @param root project root path that is appended before the path
#' @return data.table from the source csv file
s3GetFile <- function(path, header=T,
                      bucket = Sys.getenv("AWS_S3_BUCKET"),
                      root = Sys.getenv("AWS_S3_ROOT")) {
  full.path <- paste0(root, path)
  raw_data <- get_object(full.path, bucket)

  # In case of error, print error message.
  if (!is.raw(raw_data))
    stop(print(raw_data[1:3]))

  data <- iconv(readBin(raw_data, character()), from = "UTF-8", to = "UTF-8")
  df <- fread(data, header = header, strip.white = F)
  # Replace " ", "-" and "_" with "." in the header.
  names(df) <- gsub(" |_|-", ".", tolower(names(df)))
  invisible(df)
}

#' Saves data.table as csv.gz to S3
#'
#' @inherit s3PutFile
s3PutFile.gz <- function(dt, path,
                         bucket = Sys.getenv("AWS_S3_BUCKET"),
                         root = Sys.getenv("AWS_S3_ROOT")) {

  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))
  gz.connection <- gzfile(tmp.file, "w")
  write.csv(dt, file = gz.connection, row.names = F, fileEncoding = "UTF-8")
  close(gz.connection)

  full.path <- paste0(root, path)
  put_object(file = tmp.file, object = full.path, bucket = bucket)

}

#' Loads gz file from s3 and reads it as data.table from csv
#' @inherit s3PutFile
s3GetFile.gz <- function(path,
                         bucket = Sys.getenv("AWS_S3_BUCKET"),
                         root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))

  save_object(full.path, bucket, file = tmp.file)
  dt <- fread(paste0('zcat < ', tmp.file))
  invisible(dt)
}

#' Saves file as rds to S3
#'
#' @inherit s3PutFile
s3PutFile.rds <- function(dt, path,
                          bucket = Sys.getenv("AWS_S3_BUCKET"),
                          root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  s3saveRDS(dt, bucket = bucket, object = full.path)
}

#' Saves file as rds to S3
#'
#' @inherit s3GetFile
s3GetFile.rds <- function(path,
                          bucket = Sys.getenv("AWS_S3_BUCKET"),
                          root = Sys.getenv("AWS_S3_ROOT")) {

  full.path <- paste0(root, path)
  s3readRDS(dt, bucket = bucket, object = full.path)
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
