

logModelPerformance <- function(data) {

  data <- data[, c("date", "project", "model", "metric", "group", "size", "value", "dataset")]
  data <- cbind(Sys.Date(), data)

  job <- insert_upload_job(project = Sys.getenv("BIGQUERY_PROJECT"),
                             dataset = Sys.getenv("BIGQUERY_METADATA_DATASET"),
                             "model_performance",
                             data,
                             write_disposition = "WRITE_APPEND",
                             create_disposition = "CREATE_NEVER")

  res <- wait_for(job)
  return(res)

}