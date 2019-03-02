#' Saves data to a file in AWS S3
#'
#' @description Convention based wrapper functions that allow to save data in S3
#'
#' @details Environment variables required for aws.s3 access:
#'  * `AWS_ACCESS_KEY_ID` = "mykey"
#'  * `AWS_SECRET_ACCESS_KEY` = "mysecretkey"
#'  * `AWS_DEFAULT_REGION` = "us-east-1"
#'  * `AWS_SESSION_TOKEN` = "mytoken"
#'
#' @export
#' @param dt data.table to save
#' @param path S3 object path starting after root folder.
#' @param bucket name of the S3 bucket.
#'     Defaults to value in `AWS_S3_BUCKET` environment variable.#'
#' @param root project root path that is appended before the path, e.g. "/prod/".
#'     Defaults to value in `AWS_S3_ROOT` environment variable.
#' @param na.value the string to use for missing values in the data.
#'     Defaults to empty string.
#' @param ... additional parameters that will be passed into extension specific call.
#' @return TRUE if data was saved to S3.
#'
#' @seealso aws.s3 package documentation for access details:
#'   \url{https://github.com/cloudyr/aws.s3}
#' @import aws.s3
#' @md
s3PutFile <- function(dt,
                      path,
                      bucket = s3DefaultBucket(),
                      root = s3DefaultRoot(),
                      ...) {
  args <- c(as.list(environment()), list(...))
  do.call(
    what = s3PutFile.factory(path),
    args = args
  )
}

s3DefaultBucket <- function() {
  Sys.getenv("AWS_S3_BUCKET")
}

s3DefaultRoot <- function() {
  Sys.getenv("AWS_S3_ROOT")
}

s3PutFile.factory <- function(path) { # nolint
  if (grepl("\\.csv$", path)) {
    return(s3PutFile.csv)
  }
  if (grepl("\\.json.gz$", path)) {
    return(s3PutFile.json.gz)
  }
  if (grepl("(\\.|\\.csv\\.)gz$", path)) {
    return(s3PutFile.gz)
  }
  if (grepl("\\.rds$", path)) {
    return(s3PutFile.rds)
  }
  stop(paste0(
    "Unsupported file extension in the path of the s3PutFile call: ",
    tools::file_ext(path)
  ))
}

#' @export
#' @rdname s3PutFile
#' @return `s3PutFile.csv` saves data into `.csv` file in S3.
s3PutFile.csv <- function(dt, # nolint
                          path,
                          bucket = s3DefaultBucket(),
                          root = s3DefaultRoot(),
                          na.value = "") {
  tmp.file <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp.file))

  write.csv(
    dt,
    file = tmp.file,
    row.names = FALSE,
    fileEncoding = "UTF-8",
    na = na.value
  )

  full.path <- paste0(root, path)
  put_object(
    file = tmp.file,
    object = full.path,
    bucket = bucket,
    check_region = FALSE
  )
}

#' Loads data from AWS S3 into data.table object
#'
#' Convention based wrapper functions that allow to load
#' data files in S3 into data.table
#'
#' @details Environment variables required for aws.s3 access:
#'  * `AWS_ACCESS_KEY_ID` = "mykey"
#'  * `AWS_SECRET_ACCESS_KEY` = "mysecretkey"
#'  * `AWS_DEFAULT_REGION` = "us-east-1"
#'  * `AWS_SESSION_TOKEN` = "mytoken"
#'
#' `s3GetFile` calls extension specific files based on the path of the file.
#'
#' @export
#' @param path is the path to the S3 object
#' @param header flag defines whether file has header
#' @param bucket name of the S3 bucket.
#'     Defaults to value in `AWS_S3_BUCKET` environment variable.
#' @param root project root path that is appended before the path in the argument.
#'     Defaults to value in `AWS_S3_ROOT` environment variable.
#' @param ... additional arguments that will be passed to extension specific calls.
#' @return `s3GetFile` gets data from source `.csv` file
#' @seealso aws.s3 package documentation for access details:
#'   \url{https://github.com/cloudyr/aws.s3}
#' @import aws.s3
#' @importFrom data.table fread
#' @md
s3GetFile <- function(path,
                      bucket = s3DefaultBucket(),
                      root = s3DefaultRoot(),
                      ...) {
  args <- c(as.list(environment()), list(...))
  do.call(
    what = s3GetFile.factory(path),
    args = args
  )
}

s3GetFile.factory <- function(path) { # nolint
  if (grepl("\\.csv$", path)) {
    return(s3GetFile.csv)
  }
  if (grepl("\\.json.gz$", path)) {
    return(s3GetFile.json.gz)
  }
  if (grepl("(\\.|\\.csv\\.)gz$", path)) {
    return(s3GetFile.gz)
  }
  if (grepl("\\.zip$", path)) {
    return(s3GetFile.zip)
  }
  if (grepl("\\.rds$", path)) {
    return(s3GetFile.rds)
  }
  stop(paste0(
    "Unsupported file extension in the path of s3GetFile call: ",
    tools::file_ext(path)
  ))
}

#' @export
#' @rdname s3GetFile
#' @return `s3GetFile.csv` loads data from `.csv` files
s3GetFile.csv <- function(path, # nolint
                          bucket = s3DefaultBucket(),
                          root = s3DefaultRoot(),
                          header = TRUE) {
  full.path <- paste0(root, path)
  raw.data <- get_object(
    object = full.path,
    bucket = bucket,
    check_region = FALSE
  )

  # In case of error, print error message.
  if (!is.raw(raw.data)) {
    stop(print(raw.data[1:3]))
  }

  data <- iconv(
    readBin(raw.data, character()),
    from = "UTF-8",
    to = "UTF-8"
  )
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
#' @param s3.get.fun Function that will be used to read data from the individual files.
#' @param fill see data.table::rbindList
#' @return data.table with data combined from files. It will be Null data.table if path
#' did not match any of the files in S3 bucket.
s3GetData <- function(path, header = TRUE,
                      bucket = s3DefaultBucket(),
                      root = s3DefaultRoot(),
                      s3.get.fun = s3GetFile,
                      fill = TRUE) {
  full.path <- paste0(root, path)
  full.path <- gsub("^/", "", full.path)
  objects <- aws.s3::get_bucket(
    bucket = bucket,
    prefix = full.path,
    check_region = FALSE
  )
  dt.list <- lapply(objects, function(o) {
    s3.get.fun(o$Key, bucket = bucket, root = "")
  })

  invisible(
    rbindlist(
      dt.list,
      use.names = TRUE,
      fill = fill
    )
  )
}

#' Lists files that match given path
#'
#' @export
#' @inheritParams s3GetFile
#' @return metadata of files in data.table
s3ListFiles <- function(path,
                        bucket = s3DefaultBucket(),
                        root = s3DefaultRoot()) {
  full.path <- paste0(root, path)
  full.path <- gsub("^/", "", full.path)
  objects <- aws.s3::get_bucket(
    bucket = bucket,
    prefix = full.path,
    check_region = FALSE
  )
  as.data.table(objects)
}

#' @export
#' @rdname s3PutFile
#' @return `s3PutFile.gz` saves data into `.gz` (csv archived) file in S3.
s3PutFile.gz <- function(dt, path, # nolint
                         bucket = s3DefaultBucket(),
                         root = s3DefaultRoot(),
                         na.value = "") {
  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))
  gz.connection <- gzfile(tmp.file, "w")

  write.csv(
    dt,
    file = gz.connection,
    row.names = FALSE,
    fileEncoding = "UTF-8",
    na = na.value
  )

  close(gz.connection)

  full.path <- paste0(root, path)
  put_object(
    file = tmp.file,
    object = full.path,
    bucket = bucket,
    check_region = FALSE
  )
}


#' @export
#' @rdname s3GetFile
#' @return `s3GetFile.gz` loads data from `.gz` files
s3GetFile.gz <- function(path, # nolint
                         bucket = s3DefaultBucket(),
                         root = s3DefaultRoot()) {
  full.path <- paste0(root, path)
  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))

  save_object(
    object = full.path,
    bucket = bucket,
    file = tmp.file,
    check_region = FALSE
  )
  dt <- fread(paste0("zcat < ", tmp.file))
  names(dt) <- conformHeader(names(dt))
  invisible(dt)
}

#' @export
#' @rdname s3PutFile
#' @return `s3PutFile.rds` saves data as `.rds` file in S3
s3PutFile.rds <- function(dt, path, # nolint
                          bucket = s3DefaultBucket(),
                          root = s3DefaultRoot()) {
  full.path <- paste0(root, path)
  s3saveRDS(
    x = dt,
    bucket = bucket,
    object = full.path,
    check_region = FALSE
  )
}

#' @export
#' @rdname s3GetFile
#' @return `s3GetFile.rds` loads data from `.rds` files
s3GetFile.rds <- function(path, # nolint
                          bucket = s3DefaultBucket(),
                          root = s3DefaultRoot()) {
  full.path <- paste0(root, path)
  s3readRDS(
    bucket = bucket,
    object = full.path,
    check_region = FALSE
  )
}

#' @rdname s3GetFile
#' @export
#' @return `s3GetFile.zip` loads data from `.zip` files
#' @importFrom utils unzip
s3GetFile.zip <- function(path, # nolint
                          bucket = s3DefaultBucket(),
                          root = s3DefaultRoot()) {
  full.path <- paste0(root, path)
  tmp.file <- tempfile(fileext = ".zip")
  on.exit(unlink(tmp.file))

  save_object(
    object = full.path,
    bucket = bucket,
    file = tmp.file,
    check_region = FALSE
  )
  dt <- fread(unzip(tmp.file), fill = TRUE)
  names(dt) <- conformHeader(names(dt))
  invisible(dt)
}

#' @rdname s3PutFile
#' @export
#' @return `s3PutFile.json.gz` saves data as `.json.gz` (json archived) file in S3
#' @importFrom jsonlite write_json
s3PutFile.json.gz <- function(dt, path, # nolint
                              bucket = s3DefaultBucket(),
                              root = s3DefaultRoot()) {
  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))
  gz.connection <- gzfile(tmp.file, "w")
  write_json(dt, path = gz.connection)
  close(gz.connection)

  full.path <- paste0(root, path)
  put_object(
    file = tmp.file,
    object = full.path,
    bucket = bucket,
    check_region = FALSE
  )
}

#' @rdname s3GetFile
#' @export
#' @return `s3GetFile.json.gz` loads data from `.json.gz` files
#' @import data.table
#' @importFrom jsonlite fromJSON
s3GetFile.json.gz <- function(path, # nolint
                              bucket = s3DefaultBucket(),
                              root = s3DefaultRoot()) {
  full.path <- paste0(root, path)
  tmp.file <- tempfile(fileext = ".gz")
  on.exit(unlink(tmp.file))

  save_object(
    object = full.path,
    bucket = bucket,
    file = tmp.file,
    check_region = FALSE
  )
  dt <- fromJSON(tmp.file)
  dt <- data.table(dt)
  names(dt) <- conformHeader(names(dt))
  invisible(dt)
}

#' Gets the path to the object based on the root path setup via
#' environment variable
#'
#' @export
#' @param relative.path defines the path to the object post project root path.
s3GetObjectPath <- function(relative.path) {
  path <- paste0(s3DefaultRoot(), relative.path)
  return(path)
}
