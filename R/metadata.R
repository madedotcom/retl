#' Checks if ETL job exists in the etl_jobs table
#'
#' @export
#' @param job name of the ETL job
etlJobExists <- function(job) {

  table.exists <- exists_table(Sys.getenv("BIGQUERY_PROJECT"),
                               dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                               "etl_jobs")
  if (!table.exists) {
    return (FALSE)
  }

  sql.tempalte <-
    "SELECT *
     FROM etl_jobs
     WHERE job = '%1$s'"
  sql <- sprintf(sql.tempalte, job)

  # Check if job details exist:
  data <- query_exec(sql,
                     project = Sys.getenv("BIGQUERY_PROJECT"),
                     default_dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                     page_size = 1)

  if(nrow(data) > 0) {
    return (TRUE)
  }
  else {
    return (FALSE)
  }
}

#' Adds ETL job
#'
#' @export
#' @param job name of the job
#' @param increment.name name of the increment
#' @param increment.type type of the increment
etlAddJob <- function(job, increment.name, increment.type) {

  if(etlJobExists(job)) {
    return (FALSE)
  }

  etl.jobs <- data.frame(list(job = job,
                              increment_name = increment.name,
                              increment_type =increment.type), stringsAsFactors = F)

  job <- insert_upload_job(project = Sys.getenv("BIGQUERY_PROJECT"),
                           dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                           table = "etl_jobs",
                           values = etl.jobs,
                           write_disposition = "WRITE_APPEND",
                           create_disposition = "CREATE_IF_NEEDED")
  res <- wait_for(job)
  return (res)
}

#' Logs execution of the ETL job
#'
#' @export
#' @param job name of the etl job
#' @param increment.value maximum value in the field that used for increment
#' @param records number of records processed by execution
etlLogExecution <- function(job, increment.value, records = 0) {
  # Logs job execution with increment and number of recors.

  etl.increments <- data.frame(list(job = job,
                                    increment_value = increment.value,
                                    records = as.integer(records),
                                    datetime = Sys.time()),
                               stringsAsFactors = F)

  job <- insert_upload_job(project = Sys.getenv("BIGQUERY_PROJECT"),
                           dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                           table = "etl_increments",
                           values = etl.increments,
                           write_disposition = "WRITE_APPEND",
                           create_disposition = "CREATE_NEVER")
  res <- wait_for(job)
  return (res)
}

#' Gets increment value from
#'
#' @export
#' @param job name of the etl job
etlGetIncrementType <- function(job) {
  # Gets the TYPE of increment for the given job.

  sql.template <-
    "SELECT increment_type
     FROM etl_jobs
     WHERE job = '%1$s'
     LIMIT 1"
  sql <- sprintf(sql.tempalte, job)
  data <- query_exec(sql,
                     project = Sys.getenv("BIGQUERY_PROJECT"),
                     default_dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                     page_size = 1000)
  if(nrow(data) == 0) {
    return (NULL)
  }

  return(data[1, ]$increment_type)
}

#' Gets latest increment for the
#'
#' @export
#' @param job name of the job
#' @return increment value for the last execution of the job
etlGetIncrement <- function(job) {
  # Gets the latest increment for a given job.

  sql.tempalte <-
    "SELECT * FROM etl_increments
     WHERE job = '%1$s'
     ORDER BY datetime DESC
     LIMIT 1"
  sql <- sprintf(sql.tempalte, job)
  data <- query_exec(sql,
                     project = Sys.getenv("BIGQUERY_PROJECT"),
                     default_dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                     page_size = 1000)
  if(nrow(data) == 0) {
    return (as.integer(0))
  }
  return(data$increment_value)
}
