#' Functions that execute query against BigQuery database
#'
#' Execute templated query given as string or file.
#' Placeholders in template are replaced with values provided in ellipsis parameter with sprintf
#'
#' @name bqQuery
#' @rdname bqQuery
#'
#' @export
#' @param file file with sql statement
bqExecuteFile <- function(file, ...) {
  # Function to load data from BigQuery using file with SQL.
  sql <- paste(readLines(file), collapse = "\n")
  bqExecuteSql(sql, ...)
}

#' This is a helper function to support `with_mock_bigquery()`
#' @noRd
bqPrepareQuery <- function(query) {
  query
}

#' @rdname bqQuery
#'
#' @export
#' @param query template of the sql statement
bqExecuteQuery <- function(query, ...) {
  bqExecuteSql(query, ...)
}


#' @rdname bqQuery
#'
#' @export
#' @param sql string with sql statement or query template
#' @param ... any parameters that will be used to replace placeholders in the query template
#' @param use.legacy.sql switches SQL dialect.
#'   Defaults to value set in `BIGQUERY_LEGACY_SQL` env.
bqExecuteSql <- function(sql, ..., use.legacy.sql = bqUseLegacySql()) {
  # Extract named arguments and turn them into query params
  args <- c(as.list(environment()), list(...))
  args.reserved <- c("sql", "use.legacy.sql")

  params <- namedArguments(args, args.reserved)

  if (!use.legacy.sql) {
    assert_that(
      !(length(params) > 0L & nonameItemsCount(args) > 0L),
      msg = "Don't mix named and anonymous parameters in the call."
    )
  }

  if (nonameItemsCount(args) > 0L) {
    # template requires parameters.
    sql <- sprintf(sql, ...)
  } else {
    # template does not have parameteres.
    sql <- sql
  }


  if (!use.legacy.sql) {
    message_params(params)
  } else {
    params <- NULL
  }


  bqAuth()
  ds <- bigrquery::bq_dataset(
    project = bqDefaultProject(),
    dataset = bqDefaultDataset()
  )
  tb <- bigrquery::bq_dataset_query(
    x = ds,
    query = bqPrepareQuery(sql),
    billing = bqBillingProject(),
    use_legacy_sql = use.legacy.sql,
    parameters = params
  )
  dt <- data.table(bigrquery::bq_table_download(tb))
  colnames(dt) <- conformHeader(colnames(dt))
  dt
}


#' @description
#' `bqExecuteDml()` - Executes DML query without loading results
#'
#' @param priority sets priority of job execution to INTERACTIVE or BATCH
#' @rdname bqQuery
#' @export
bqExecuteDml <- function(query, ...,
                         priority = "INTERACTIVE") {
  bqAuth()

  # Extract named arguments and turn them into query params
  args <- c(as.list(environment()), list(...))
  args.reserved <- c("query", "priority")

  params <- namedArguments(args, args.reserved)

  assert_that(
    !(length(params) > 0L & nonameItemsCount(args) > 0L),
    msg = "All parameters must be named."
  )

  message_params(params)

  ds <- bq_dataset(
    project = bqDefaultProject(),
    dataset = bqDefaultDataset()
  )

  job <- bq_perform_query(
    query = query,
    billing = bqBillingProject(),
    default_dataset = ds,
    use_legacy_sql = FALSE,
    priority = priority,
    parameters = params
  )
  if (priority == "INTERACTIVE") {
    bq_job_wait(job)
  }
  else {
    job
  }
}

#' Download query output via Storage
#'
#' `bqDownloadQuery()` Only works with Standard SQL
#'
#' @rdname bqQuery
#' @export
bqDownloadQuery <- function(query, ...) {
  export.format <- "CSV"
  export.compression <- "GZIP"

  googleCloudStorageR::gcs_global_bucket(Sys.getenv("GCS_BUCKET"))

  # Execute Query to get results into a temporary table
  job <- bqExecuteDml(query, ...)
  job.meta <- bigrquery::bq_job_meta(job)
  bq.table <- job.meta$configuration$query$destinationTable

  # Export temporary table to Storage
  bqExtractTable(
    table = bq.table$tableId,
    dataset = bq.table$datasetId,
    format = export.format,
    compression = export.compression
  )

  # Load data from Storage to data.table
  temp.file.path <- tempfile(fileext = ".csv.gz")
  on.exit({
    unlink(temp.file.path)
    googleCloudStorageR::gcs_delete_object(
      gsUri(
        bq.table,
        format = export.format,
        compression = export.compression
      )
    )
  })

  # googleCloudStorageR::gcs_auth(json_file = Sys.getenv("GCS_AUTH_FILE"))
  googleCloudStorageR::gcs_get_object(
    gsUri(bq.table, format = export.format, compression = export.compression),
    saveToDisk = temp.file.path
  )
  dt <- fread(temp.file.path)
  colnames(dt) <- conformHeader(colnames(dt))
  dt
}

#' Download results of query from a file via Storage
#'
#' @rdname bqQuery
#' @export
bqDownloadQueryFile <- function(file, ...) {
  query <- paste(readLines(file), collapse = "\n")
  bqDownloadQuery(query, ...)
}


#' Returns subset of arguments where name is set excluding reserved names
#'
#' @noRd
namedArguments <- function(args, reserved) {
  args.names <- names(args)
  args.names <- args.names[nchar(args.names) > 0 & !(args.names %in% reserved)]
  params <- args[args.names]
  params
}

#' Counts number of items in the list that don't have names
#'
#' @noRd
nonameItemsCount <- function(x) {
  sum(nchar(names(x)) == 0)
}


#' Creates table using given sql
#'
#' @export
#' @param table name of a table to be created
#' @param dataset name of the destination dataset
#' @param ... list of parameters for query template. With Standard sql all parameters must be named.
#'  With Legacy SQL ` sprintf()` will be applied for the query template.
#' @param write.disposition defines whether records will be appended
#' @param priority sets priority of job execution to INTERACTIVE or BATCH
#' @param schema.file sets path to schema file for initialisation
#' @inheritParams bqExecuteSql
#' @return results of the execution as returned by bigrquery::query_exec
bqCreateTable <- function(sql,
                          table,
                          ...,
                          dataset = bqDefaultDataset(),
                          write.disposition = "WRITE_APPEND",
                          priority = "INTERACTIVE",
                          use.legacy.sql = bqUseLegacySql(),
                          schema.file = NULL) {
  bqAuth()

  if (missing(schema.file) || is.null(schema.file)) {
    message("No schema file was passed")
  } else {
    message("Schema file passed. Initiating table.")
    bqInitiateTable(
      table = table,
      schema.file = schema.file,
      dataset = dataset
    )
    message("Initiated successfully")
  }

  args <- c(as.list(environment()), list(...))

  if (use.legacy.sql) {
    job <- bqCreateTableLegacy(
      sql = sql,
      table = table,
      dataset = dataset,
      write.disposition = write.disposition,
      priority = priority
    )
  } else {

    # Extract named arguments and turn them into query params
    args.reserved <- c(
      "sql",
      "table",
      "dataset",
      "write.disposition",
      "priority",
      "use.legacy.sql",
      "schema.file"
    )
    params <- namedArguments(args, args.reserved)

    assert_that(
      !(length(params) > 0L & nonameItemsCount(args) > 0L),
      msg = "All parameters must be named."
    )
    message_params(params)

    if (write.disposition == "WRITE_TRUNCATE" && !contains_partition(table)) {
      # https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
      # > schemaUpdateOptions[]: For normal tables, WRITE_TRUNCATE will always overwrite the schema.

      # Partition tables are excluded from this flow as schema is not affected
      # by query results being saved into a partition.

      bq.table <-  bq_table(
        project = Sys.getenv("BIGQUERY_PROJECT"),
        dataset = dataset,
        table = table
      )
      if (bq_table_exists(bq.table)) {
        table.meta <- bq_table_meta(bq.table)
        on.exit(
          # compensate for unwanted behaviour of BigQuery
          bq_table_patch(bq.table, table.meta$schema$fields)

          # This is not ideal as it will not fail early and table
          # can be overwritten with wrong schema and fail on patch (which is too late)

          # @byapparov:
          # I have also tried `CREATE OR REPLACE  DML approach:
          # https://stackoverflow.com/a/58538111/599911
          # This does not work for partitioned tables hence was abondoned.
        )
      }
    }

    tbl <- bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )

    ds <- bq_dataset(
      project = bqDefaultProject(),
      dataset = dataset
    )

    job <- bq_perform_query(
      query = sql,
      billing = bqBillingProject(),
      destination_table = tbl,
      default_dataset = ds,
      create_disposition = "CREATE_IF_NEEDED",
      write_disposition = write.disposition,
      use_legacy_sql = FALSE,
      priority = priority,
      parameters = params
    )
  }

  if (priority == "INTERACTIVE") {
    bq_job_wait(job)
  }
  else {
    job
  }
}


#' This is helper function to create table from legacy SQL query
#' @noRd
bqCreateTableLegacy <- function(sql,
                                table,
                                dataset,
                                write.disposition,
                                priority) {

  tbl <- bq_table(
    project = bqDefaultProject(),
    dataset = dataset,
    table = table
  )

  ds <- bq_dataset(
    project = bqDefaultProject(),
    dataset = dataset
  )

  bq_perform_query(
    query = sql,
    billing = bqBillingProject(),
    destination_table = tbl,
    default_dataset = ds,
    create_disposition = "CREATE_IF_NEEDED",
    write_disposition = write.disposition,
    use_legacy_sql = TRUE,
    priority = priority
  )
}


message_params <- function(x) { # nolint
  if (length(x) > 0) {
    message(
      "parameters applied to the template: \n",
      jsonlite::toJSON(x, auto_unbox = TRUE, force = TRUE)
    )
  }
}

contains_partition <- function(x) grepl("\\$", x) # nolint
