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


#' Saves the data frame to S3.
#'
#' @param df data to save
#' @param file.name name of the file to be uploaded to S3.
#' @param path S3 path starting after root folder, excludes filename. example: "folder/"
#' @param bucket S3 bucket
#' @param root Root folder that will be appended before the path
s3PutFile <- function (df, file.name, path="",
                       bucket = Sys.getenv("AWS_S3_BUCKET"),
                       root = Sys.getenv("AWS_S3_ROOT")) {

  write.csv(df, file = file.name, row.names = F)
  full.path <- paste0(root, path, file.name)
  put_object(file = file.name, object = full.path, bucket=bucket)
  if (file.exists(file.name)) file.remove(file.name)
}


#' Loads data from csv file in AWS S3
#'
#' @param path is the path to the S3 object
#' @param header flag defines whether file has header
#' @param bucket name of the bucket
#' @param root project root path that is appended before the path
#'
s3GetFile <- function(path, header=T,
                      bucket = Sys.getenv("AWS_S3_BUCKET"),
                      root = Sys.getenv("AWS_S3_ROOT")) {
  # Args:
  #   path: S3 path starting after root folder, includes filename. example: "folder/file.ext".
  full.path <- paste0(root, path)
  raw_data <- get_object(full.path, bucket)

  # In case of error, print error message.
  if(!is.raw(raw_data))
    stop(print(raw_data[1:3]))

  data <- iconv(readBin(raw_data, character()), from="UTF-8", to="UTF-8")
  df <- as.data.frame(fread(data, header=header, strip.white = F))
  # Replace " ", "-" and "_" with "." in the header.
  names(df) <- gsub(" |_|-", ".", names(df))
  return(df)
}
