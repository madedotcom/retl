#' Exports BigQuery table into Google Cloud Storage file
#'
#' @export
#'
#' @param table name of the table to extract
#' @param dataset name of the dataset
#' @param format The exported file format. Possible values
#'   include "CSV", "NEWLINE_DELIMITED_JSON" and "AVRO". Tables with nested or
#'   repeated fields cannot be exported as CSV.
#' @param compression The compression type to use for exported files. Possible
#'   values include "GZIP", "DEFLATE", "SNAPPY", and "NONE". "DEFLATE" and
#'   "SNAPPY" are only supported for Avro.
#' @seealso ?bigrquery::bq_table_save
#' @return object of `bq_job`
bqExtractTable <- function(table,
                           dataset = bqDefaultDataset(),
                           format = "CSV",
                           compression = "GZIP") {

  x <- bq_table(
    project = bqDefaultProject(),
    dataset = dataset,
    table = table
  )

  bigrquery::bq_table_save(
    x,
    destination_uris = gsUri(x, format, compression),
    destination_format = format,
    compression = compression
  )
}


#' Imports data from gs to BigQuery table
#'
#' @export
#'
#' @inherit bqExtractTable
#' @param append defines whether data can be appended to the table with data
#' @param nskip number of rows to skip on importing the file
#' @param path path to the file in gs, defaults to {default-bucket}/{default-dataset}/table-name.csv.gz.
bqImportData <- function(table,
                         dataset = bqDefaultDataset(),
                         path = "",
                         append = TRUE,
                         format = "CSV",
                         compression = "GZIP",
                         nskip = 1) {

  write.disposition <- ifelse(append, "WRITE_APPEND", "WRITE_TRUNCATE")
  x <- bq_table(
    project = bqDefaultProject(),
    dataset = dataset,
    table = table
  )

  if (path == "") {
    path <- gsTablePath(x, format, compression)
  }

  if (bqTableExists(table, dataset)) {
    table.schema <- bq_table_fields(x)
  } else {
    table.schema <- NULL
  }

  bigrquery::bq_table_load(
    x,
    source_uris = gsPathUri(path),
    source_format = format,
    write_disposition = write.disposition,
    nskip = 1,
    fields = table.schema
  )

}

#' makes full path to the gs file from relative part
#'
#' @param path relative path to the gs object
gsPathUri <- function(path) {
  paste0(
    "gs://",
    s3DefaultBucket(), "/",
    gsub("/", "", s3DefaultRoot()), "/",
    path
  )
}

#' makes relative path to a file to mirror table path
#'
#' @inherit gsUri
gsTablePath <- function(x, format, compression) {
  extension <- extensionFromFormat(format, compression)
  paste0(
    x$dataset, "/",
    x$table, ".", extension
  )
}

#' Gets gs uri to mirror table by name
#'
#' @param x bq_table object
#' @param format format of the file
#' @param compression compression applied to the filed
gsUri <- function(x, format, compression) {
  gsPathUri(
    gsTablePath(x, format, compression)
  )
}

#' Converts possible BigQuery extract formats to file extensions
#' @param format format of the file
#' @param compression level of compression to apply to the output
#' @return corresponding file extension
extensionFromFormat <- function(format, compression = "NONE") {
  if (format == "CSV") {
    if (compression == "GZIP") {
      "csv.gz"
    }
    else {
      "csv"
    }
  }
  else if (format == "NEWLINE_DELIMITED_JSON") {
    if (compression == "GZIP") {
      "jzon.gzip"
    }
    else {
      "json"
    }
  }
  else if (format == "AVRO") {
    "avro"
  }
  else {
    stop("Format is unsupported by BigQuery extract: ", format)
  }
}
