
#' Sets description of the table
#'
#' @param table name of the table to update
#' @param description character scalar with table description
#' @param dataset name of the dataset
#' @param project name of the project
bqUpdateTableDescription <- function(table, description, dataset = bqDefaultDataset(), project = bqDefaultProject()) {
  bqAlterTable(
    bigrquery::bq_table(
      project = project,
      dataset = dataset,
      table = table
    ),
    name = "description",
    value = description
  )
}


#' Executes ALTER TABLE DML command to set table option value
#'
#' @param x bq_table object
#' @param name name of the option
#' @param value value of the option
#'
#' @seealso https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_set_options_statement
bqAlterTable <- function(x, name, value) {
  dml <- glue(
    "ALTER TABLE `{project}.{dataset}.{table}`
    SET OPTIONS (
      {option} = @description
    )",
    project = x$project,
    dataset = x$dataset,
    table = x$table,
    option = name
  )
  dml <- as.character(dml)
  bqExecuteDml(dml, description = value)
}
