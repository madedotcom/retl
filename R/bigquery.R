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
#' @return results of execution
bqInsertLargeData <- function(table,
                              data,
                              dataset = bqDefaultDataset(),
                              chunks = 5,
                              append = TRUE, 
                              schema.file = NULL) {

  upload.list <- split(
    data,
    f = sort(sample(1:chunks, size = nrow(data), replace = TRUE))
  )

  if (length(upload.list) == 0) {
    return(NULL)
  }

  bqInsertData(
      table = table,
      dataset = dataset,
      data = upload.list[[1]],
      append = append,
      schema.file = schema.file
  )

  lapply(upload.list[-1], function(dt) {
    bqInsertData(
      table = table,
      dataset = dataset,
      data = dt,
      append = TRUE
    )
  })
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
    print("Initiated successfully")
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
