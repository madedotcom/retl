library(bigrquery)

if (bqTokenFileValid()) {
  bqAuth()
  ds <- bq_test_dataset()
  Sys.setenv(BIGQUERY_DATASET = ds$dataset)
  Sys.setenv(BIGQUERY_PROJECT = Sys.getenv("BIGQUERY_TEST_PROJECT"))
  if (!bqDatasetExists()) {
    bqCreateDataset()
  }
}


