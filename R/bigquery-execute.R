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


  if (!use.legacy.sql & length(params) > 0) {
    cat("parameters applied to the template: \n")
    print(jsonlite::toJSON(params, auto_unbox = TRUE, force = TRUE))
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
    query = sql,
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

  if (length(params) > 0) {
    cat("parameters applied to the template: \n")
    print(jsonlite::toJSON(params, auto_unbox = TRUE, force = TRUE))
  }

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
#' @rdname bqQuery
#' @export
bqDownloadQuery <- function(query, ...) {
  export.format <- "CSV"
  export.compression <- "GZIP"

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
  temp.file.path <- paste0(tempfile(), ".csv.gz")
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

  googleCloudStorageR::gcs_auth()
  googleCloudStorageR::gcs_get_object(
    gsUri(bq.table, format = export.format, compression = export.compression),
    saveToDisk = temp.file.path
  )
  dt <- fread(temp.file.path)
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
  args[args.names]
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
#' @param write.disposition defines whether records will be appended
#' @param priority sets priority of job execution to INTERACTIVE or BATCH
#' @inheritParams bqExecuteSql
#' @return results of the execution as returned by bigrquery::query_exec
bqCreateTable <- function(sql,
                          table,
                          dataset = bqDefaultDataset(),
                          write.disposition = "WRITE_APPEND",
                          priority = "INTERACTIVE",
                          use.legacy.sql = bqUseLegacySql()) {
  bqAuth()
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
    use_legacy_sql = use.legacy.sql,
    priority = priority
  )
  if (priority == "INTERACTIVE") {
    bq_job_wait(job)
  }
  else {
    job
  }
}
