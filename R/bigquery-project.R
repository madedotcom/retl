#' Functions to work with BigQuery projects
#'
#' Family of functions for common operations on projects
#'
#' @name bqProject
#' @param project name of the bigquery project
NULL

#' Gets list of datasets for a given project
#' @rdname bqProject
bqProjectDatasets <- function(project = bqDefaultProject()) {
  bq_project_datasets(
    x = project,
    max_pages = Inf
  )
}

#' Gets metadata of all tables of a project into a data.table
#'
#' @export
#'
#' @details
#' Function queries __TABLES__ table for each dataset in the project
#'
#' @rdname bqProject
#' @param datasets list of dataset object to filter results
bqProjectTables <- function(project = bqDefaultProject(),
                            datasets = bqProjectDatasets()) {
  if (bqUseLegacySql()) {
    sql <- "
    SELECT
      project_id,
      dataset_id,
      table_id,
      TIMESTAMP(creation_time / 1000) as creation_time,
      TIMESTAMP(last_modified_time / 1000) as last_modified_time,
      row_count,
      size_bytes,
      type
    FROM
      [%1$s.__TABLES__]"
  }
  else {
    sql <- "
    SELECT
      project_id,
      dataset_id,
      table_id,
      TIMESTAMP_MILLIS(creation_time) as creation_time,
      TIMESTAMP_MILLIS(last_modified_time) as last_modified_time,
      row_count,
      size_bytes,
      type
    FROM `%1$s.__TABLES__`"
  }

  tables <- lapply(datasets, function(d) {
    dt <- bqExecuteQuery(sql, d$dataset)
    meta <- bq_dataset_meta(
      bq_dataset(
        project = project,
        dataset = d$dataset
      ),
      fields = c("location")
    )
    dt$location <- meta$location
    dt
  })
  tables <- rbindlist(tables)
}
