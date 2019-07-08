if (gargle:::secret_pw_exists("retl")) {
  print("Have password for retl")
} else {
  stop("No password retl")
}

if (gargle:::secret_can_decrypt("retl")) {
  print("Can decrypt retl")
  json <- gargle:::secret_read(package = "retl", name = "retl-testing.json")
  bigrquery::bq_auth(path = rawToChar(json))

  ds <- bigrquery::bq_test_dataset()
  Sys.setenv(BIGQUERY_DATASET = ds$dataset)
  Sys.setenv(BIGQUERY_PROJECT = Sys.getenv("BIGQUERY_TEST_PROJECT"))
  if (!bqDatasetExists()) {
    "Created test dataset"
    bqCreateDataset()
  }
} else {
  stop("Cannot decrypt retl")
}
