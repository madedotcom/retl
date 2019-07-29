#' Dataset manipulation functions
#'
#' @rdname bqDataset
#'
#' @export
#' @param dataset name of the dataset
#' @param project name of the project
#' @return `bqDatasetExists()` - returns TRUE if dataset exists
bqDatasetExists <- function(dataset = bqDefaultDataset(),
                            project = bqDefaultProject()) {
  bqAuth()
  ds <- bq_dataset(project, dataset)
  bq_dataset_exists(ds)
}

#' @rdname bqDataset
#' @export
bqCreateDataset <- function(dataset = bqDefaultDataset(),
                            project = bqDefaultProject()) {
  bqAuth()
  bq_dataset_create(
    bq_dataset(
      project = project,
      dataset = dataset
    )
  )
}

#' @rdname bqDataset
#' @details `bqDeleteDataset()` - You can protect dataset
#'   from programmatic deletion by adding `delete:never` label (key:value) to it.
#' @export
#' @param delete.contents removes all content from the dataset if is set to TRUE
bqDeleteDataset <- function(dataset = bqDefaultDataset(),
                            project = bqDefaultProject(),
                            delete.contents = FALSE) {
  bqAuth()

  assert_that(!bqProtectedDataset(dataset, project))

  bq_dataset_delete(
    bq_dataset(
      project = project,
      dataset = dataset
    ),
    delete_contents = delete.contents
  )
}

#' Gets list of tables for a given dataset
#' @export
#' @rdname bqDataset
bqDatasetTables <- function(dataset = bqDefaultDataset(),
                            project = bqDefaultProject()) {
  bq_dataset_tables(
    bq_dataset(
      project = project,
      dataset = dataset
    )
  )
}

#' Checks whether dataset has delete:never label pair attached
#' @noRd
bqProtectedDataset <- function(dataset, project) {
  meta <- bq_dataset_meta(
    bq_dataset(
      project = project,
      dataset = dataset
    ),
    fields = c("labels")
  )
  if (length(meta) > 0) {
    delete <- meta$labels$delete
    if (!is.null(delete) && delete == "never") {
      TRUE
    }
    else {
      FALSE
    }
  }
  else {
    FALSE
  }
}

#' Function to get list of datasets matching given pattern
#'
#' @param pattern regex to match
#' @return list of dataset objects (bigrquery)
bqMatchDatasets <- function(pattern) {
  
  bqAuth()
  datasets <- bq_project_datasets(Sys.getenv("BIGQUERY_PROJECT"))
  Filter(function(d) grepl(pattern, d$dataset), datasets)

}
