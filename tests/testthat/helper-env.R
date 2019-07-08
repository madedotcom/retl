if (gargle:::secret_can_decrypt("retl")) {
  print("Can decrypt")
  json <- gargle:::secret_read(package = "retl", name = "retl-testing.json")
  bigrquery::bq_auth(path = rawToChar(json))
}

if (bqTokenFileValid()) {
  bqAuth()
  ds <- bigrquery::bq_test_dataset()
  Sys.setenv(BIGQUERY_DATASET = ds$dataset)
  Sys.setenv(BIGQUERY_PROJECT = Sys.getenv("BIGQUERY_TEST_PROJECT"))
  if (!bqDatasetExists()) {
    "Created test dataset"
    bqCreateDataset()
  }
}
