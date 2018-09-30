
#' Logs model performance data.frame to BigQuery table
#'
#' @export
#' @param data data with metrics of the model
logModelPerformance <- function(data) {

  data <- data[, c("project", "model", "metric", "group", "size", "value", "dataset")]
  data <- cbind(date = Sys.time(), data)

  job <- insert_upload_job(project = Sys.getenv("BIGQUERY_PROJECT"),
                             dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                             "model_performance",
                             data,
                             write_disposition = "WRITE_APPEND",
                             create_disposition = "CREATE_NEVER")

  res <- wait_for(job)
  return(res)

}
