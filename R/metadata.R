#' Checks if ETL job exists in the etl_jobs table
#'
#' @export
#' @param job name of the ETL job
etlJobExists <- function(job) {
  bqAuth()
  table.exists <- bq_table_exists(
    bq_table(
      project = Sys.getenv("BIGQUERY_PROJECT"),
      dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
      table = "etl_jobs"
    )
  )
  if (!table.exists) {
    return(FALSE)
  }

  sql.tempalte <-
    "SELECT *
     FROM etl_jobs
     WHERE job = '%1$s'"

  data <- etlQuery(sql.tempalte, job)

  if (nrow(data) > 0) {
    return(TRUE)
  }
  else {
    return(FALSE)
  }
}

#' Adds ETL job
#'
#' @export
#' @param job name of the job
#' @param increment.name name of the increment
#' @param increment.type type of the increment
etlAddJob <- function(job, increment.name, increment.type) {
  bqAuth()
  if (etlJobExists(job)) {
    return(FALSE)
  }

  etl.jobs <- data.frame(
    list(
      job = job,
      increment_name = increment.name,
      increment_type = increment.type
    ),
    stringsAsFactors = F
  )

  bqInsertData(
    table = "etl_jobs",
    data = etl.jobs,
    dataset = Sys.getenv("BIGQUERY_METADATA_DATASET")
  )
}

#' Logs execution of the ETL job with increment and number of recors.
#'
#' @export
#' @param job name of the etl job
#' @param increment.value maximum value in the field that used for increment
#' @param records number of records processed by execution
etlLogExecution <- function(job, increment.value, records = 0) {
  bqAuth()

  etl.increments <- data.frame(
    job = job,
    increment_value = as.integer(increment.value),
    records = as.integer(records),
    datetime = Sys.time()
  )

  tbl <- bigrquery::bq_table(
    project = bqDefaultProject(),
    dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
    table = "etl_increments"
  )

  job.wait <- bigrquery::bq_perform_upload(
    x = tbl,
    values = etl.increments,
    fields = as_bq_fields(etl.increments),
    write_disposition = "WRITE_APPEND",
    create_disposition = "CREATE_NEVER"
  )
  bq_job_wait(job.wait)
}

#' Gets increment value from
#'
#' @export
#' @param job name of the etl job
etlGetIncrementType <- function(job) {
  .Deprecated(msg = "Only integer increments are supported")
}

#' Gets latest increment for the job
#'
#' @export
#' @param job name of the job
#' @return increment value for the last execution of the job
etlGetIncrement <- function(job) {

  sql.tempalte <-
    "SELECT *
     FROM
        etl_increments
     WHERE
        job = '%1$s'
     ORDER BY
        datetime DESC
     LIMIT 1"
  bqAuth()

  data <- etlQuery(sql.tempalte, job)

  if (nrow(data) == 0) {
    return(as.integer(0))
  }
  return(data$increment_value)
}

etlQuery <- function(query, ...) {
  query <- sprintf(query, ...)
  ds <- bigrquery::bq_dataset(
    project = bqDefaultProject(),
    dataset = Sys.getenv("BIGQUERY_METADATA_DATASET")
  )
  tb <- bigrquery::bq_dataset_query(
    x = ds,
    query = query,
    billing = bqBillingProject(),
    use_legacy_sql = bqUseLegacySql()
  )
  bigrquery::bq_table_download(tb)
}
