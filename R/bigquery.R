#' @import utils
#' @import bigrquery
#' @import stringr
#' @import assertthat
NULL


#' Inserts LARGE data into BigQuery table
#'
#' @description
#' Truncates the target table and appends data broken in chunks
#'
#' @export
#' @param table name of the target table
#' @param data data to be inserted
#' @param dataset name of the destination dataset
#' @param append specifies if data should be appended or truncated
#' @param chunks number of segments the data should be split into
#' @param schema.file sets path to schema file for initialisation
#' @param bucket name of the GCS bucket where data will be saved in temp file.
#'   Defaults to value of environment variable `GCS_DEFAULT_BUCKET`
#' @return results of execution
bqInsertLargeData <- function(table,
                              data,
                              dataset = bqDefaultDataset(),
                              chunks = 5,
                              append = TRUE,
                              schema.file = NULL,
                              bucket = Sys.getenv("GCS_DEFAULT_BUCKET")) {

  if (nrow(data) == 0L) {
    return(NULL)
  }

  if (ncol(data) > 0) {
    colnames(data) <- conformHeader(colnames(data), "_")
    fields <- as_bq_fields(data)
  }

  temp.filename <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".json")
  temp.object <- tempfile(pattern = "data_import_", tmpdir = "tmp", fileext = ".json")

  on.exit({
    unlink(temp.filename)
    googleCloudStorageR::gcs_delete_object(
      bucket = bucket,
      object_name = temp.object
    )
  })

  # Save data to local temporary file
  jsonlite::stream_out(data, file(temp.filename))

  googleCloudStorageR::gcs_upload(
    file = temp.filename,
    bucket = bucket,
    name = temp.object,
    predefinedAcl = "projectPrivate"
  )

  bqImportData(
    table = table,
    dataset = dataset,
    path = temp.object,
    append = append,
    format = "NEWLINE_DELIMITED_JSON",
    bucket = bucket
  )
}

#' Inserts data into BigQuery table
#'
#' @export
#' @param table name of the target table
#' @param data data to be inserted
#' @param dataset name of the destination dataset
#' @param append specifies if data should be appended or truncated
#' @param fields list of fields with names and types (as bq_fields)
#' @param schema.file sets path to schema file for initialisation
#' @return results of execution
bqInsertData <- function(table,
                         data,
                         dataset = bqDefaultDataset(),
                         append = TRUE,
                         fields = NULL,
                         schema.file = NULL) {
  assert_that(
    nchar(dataset) > 0,
    msg = "Set dataset parameter or BIGQUERY_DATASET env var."
  )

  if (missing(fields) & ncol(data) > 0) {
    colnames(data) <- conformHeader(colnames(data), "_")
    fields <- as_bq_fields(data)
  }

  write.disposition  <- ifelse(append, "WRITE_APPEND", "WRITE_TRUNCATE")

  if (missing(schema.file) || is.null(schema.file)) {
    message("No schema file was passed")
  }
  else {
    message("Schema file passed. Initiating table.")
    bqInitiateTable(table = table,
                    schema.file = schema.file,
                    dataset = dataset)
    message("Initiated successfully")
    if (append == FALSE) {
      message("Truncating ", dataset, ".", table)
      bqExecuteSql("DELETE FROM %1$s.%2$s WHERE 1=1",
                   dataset,
                   table,
                   use.legacy.sql = FALSE)
      write.disposition <- "WRITE_EMPTY"
    }
  }

  rows <- nrow(data)

  if (rows > 0) {
    bqAuth()

    tbl <- bigrquery::bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )

    job <- bigrquery::bq_perform_upload(
      x = tbl,
      values = data,
      fields = fields,
      write_disposition = write.disposition,
      create_disposition = "CREATE_IF_NEEDED"
    )

    res <- bq_job_wait(job)

    return(res)
  } else {
    return(NULL)
  }
}

#' Waits for jobs provided in the list if priority is BATCH
#' @noRd
bqWait <- function(jobs, priority) {
  if (priority == "BATCH") {
    # Wait for all the jobs that were submitted
    jobs <- lapply(jobs, function(job) {
      bq_job_wait(job)
    })
  }
  invisible(jobs)
}
