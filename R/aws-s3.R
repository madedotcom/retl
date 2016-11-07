library(devtools)
library(aws.s3)


s3PutFile <- function (df, file.name=df, path="",
                       bucket = Sys.getenv("AWS_S3_BUCKET"),
                       root = Sys.getenv("AWS_S3_ROOT")) {
  # Saves the data frame in S3.
  #   file.name: name of the file to be uploaded in S3.
  #   path: S3 path starting after root folder, excludes filename. example: "folder/".

  write.csv(df, file = file.name, row.names = F)
  full.path <- paste0(root, path, file.name)
  put_object(file = file.name, object = full.path, bucket=bucket)
  if (file.exists(file.name)) file.remove(file.name)
}

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
