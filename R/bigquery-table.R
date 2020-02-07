#' Functions to work with BigQuery tables
#'
#' Family of functions for common operations on tables
#'
#' @name bqTable
NULL

#' @rdname bqTable
#'
#' @export
#' @param table name of the table
#' @param dataset name of the dataset
#' @return `bqTableExists` TRUE if table exists
bqTableExists <- function(table, dataset = bqDefaultDataset()) {
  bqAuth()
  bt <- bigrquery::bq_table(
    project = bqDefaultProject(),
    dataset = dataset,
    table
  )
  bigrquery::bq_table_exists(bt)
}

#' @rdname bqTable
#'
#' @export
#' @return `bqDeleteTable` TRUE if table was deleted
bqDeleteTable <- function(table, dataset = bqDefaultDataset()) {
  assert_that(
    nchar(dataset) > 0,
    msg = "Set dataset parameter or BIGQUERY_DATASET env var."
  )

  bqAuth()
  bt <- bq_table(
    project = bqDefaultProject(),
    dataset = dataset,
    table = table
  )
  bq_table_delete(bt)
}

#' @rdname bqTable
#' @details `bqInitiateTable()` - creates table from schema file.
#'   If table already exists it will attempt to patch the table with new fields.
#'   Will fail if schema file is missing fields compared to the target table.
#' @export
#' @importFrom jsonlite read_json
#'
#' @inheritParams bqCreateTable
#' @param schema.file path to file with the table schema
#' @param partition time partitioned table will be created if set to TRUE
#' @seealso https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#schema.fields
bqInitiateTable <- function(table,
                            schema.file,
                            partition = FALSE,
                            dataset = bqDefaultDataset(),
                            clustering = NULL) {
  bqAuth()

  if (!bqTableExists(table = table, dataset = dataset)) {
    tbl <- bigrquery::bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )

    day.partitioning <- NULL
    if (partition) day.partitioning <- list(type = "DAY")

    bigrquery::bq_table_create(
      tbl,
      fields = read_json(schema.file),
      timePartitioning = day.partitioning,
      clustering = clustering
    )
  }
  else {
    warning(paste0("Table already exists: [", dataset, ".", table, "], attempting patch."))
    bqPatchTable(table = table, schema.file = schema.file, dataset = dataset)
  }
}

#' @details `bqPatchTable()`
#'   Adds new fields to a BigQuery table from a schema file.
#'   Will raise an exception if fields are removed from the schema file,
#'   but are still present in the target table.
#'
#' @rdname bqTable
#' @export
bqPatchTable <- function(table, schema.file, dataset = bqDefaultDataset()) {
  bqAuth()

  table.schema <- bqTableSchema(table, dataset)
  code.schema <- bqReadSchema(schema.file)

  new.fields <- bqSetdiffSchemas(code.schema, table.schema)
  removed.fields <- bqSetdiffSchemas(table.schema, code.schema)

  assert_that(length(removed.fields) == 0L)

  if (length(new.fields) > 0L) {
    table <- bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )
    bq_table_patch(table, code.schema)
  }
}

#' @export
#' @rdname bqTable
bqTableSchema <- function(table, dataset = bqDefaultDataset()) {
  bqAuth()
  bq_table_fields(
    bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )
  )
}

#' Copies table in BigQuery
#'
#' @export
#' @param from name of the source table
#' @param to name of the destination table
#' @param override defines if command will override existing table if it is not empty.
#' @return TRUE if the table has been successfully copied
bqCopyTable <- function(from, to, override = TRUE) {
  bqAuth()

  src <- list(
    project_id = bqDefaultProject(),
    dataset_id = bqDefaultDataset(),
    table_id = from
  )

  dest <- list(
    project_id = bqDefaultProject(),
    dataset_id = bqDefaultDataset(),
    table_id = to
  )

  write.disposition <- ifelse(override, "WRITE_TRUNCATE", "WRITE_EMPTY")
  bq_table_copy(
    x = src,
    dest = dest,
    write_disposition = write.disposition
  )

  return(bqTableExists(to))
}
