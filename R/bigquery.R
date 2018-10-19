#' @import utils
#' @import bigrquery
#' @import stringr
#' @import assertthat
NULL

#' Wrapper for the set_service_token that uses BIGQUERY_ACCESS_TOKEN_PATH env var
#'  as default value for the secret token location.
#'
#' @description
#' Required environment variables to use BigQuery helper functions:
#'   `BIGQUERY_PROJECT`` - name of the project in BigQuery.
#'   `BIGQUERY_DATASET`` - name of the default dataset in BigQuery.
#'   `BIGQUERY_ACCESS_TOKEN_PATH` - path to the json token file.
bqAuth <- function() {
  if (!bigrquery::has_access_cred()) {
    bigrquery::set_service_token(bqTokenFile())
  }
}

bqTokenFile <- function() {
  assert_that(bqTokenFileValid())
  bqTokenFilePath()
}

bqTokenFilePath <- function() {
  Sys.getenv("BIGQUERY_ACCESS_TOKEN_PATH")
}

bqTokenFileValid <- function() {
  token.file <- bqTokenFilePath()
  token.file != "" && file.exists(token.file)
}

#' Gets existing dates for date partitioned table in BigQuery
#'
#' @export
#' @param table name of a table
#' @return string vector of dates
bqExistingPartitionDates <- function(table) {
  if (!bqTableExists(table)) {
    return(character())
  }
  sql <- bqPartitionDatesSql(table)
  res <- bqExecuteSql(sql)
  if (nrow(res) > 0) {
    return(res$partition.id)
  }
  else {
    return(character())
  }
}

#' Generates sql for extraction of existing partition date
#' @noRd
bqPartitionDatesSql <- function(table) {
  if (bqUseLegacySql()) {
    paste0("SELECT partition_id from [", table, "$__PARTITIONS_SUMMARY__];")
  } else {
    paste0("SELECT
              FORMAT_DATE('%Y%m%d', DATE(_PARTITIONTIME)) as partition_id
            FROM `", table, "`
            GROUP BY 1;")
  }
}

#' Gets the value from the corresponding environment variable as boolean
#' Determines which flavour of sql should be used by default.
#' @export
#' @param x Sets `BIGQUERY_LEGACY_SQL` variable if set. Otherwise function returns value of the variable.
bqUseLegacySql <- function(x = NULL) {
  if (is.null(x)) {
    Sys.getenv("BIGQUERY_LEGACY_SQL", unset = "TRUE") == "TRUE"
  }
  else {
    Sys.setenv("BIGQUERY_LEGACY_SQL" = x)
  }
}

bqDefaultProject <- function() {
  Sys.getenv("BIGQUERY_PROJECT")
}

bqDefaultDataset <- function() {
  Sys.getenv("BIGQUERY_DATASET")
}

bqBillingProject <- function() {
  bqDefaultProject()
}

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


#' Gets dates that are missing from the date range for a give list of existing dates
#'
#' @export
#' @param start.date begining of the period
#' @param end.date end of the period
#' @param existing.dates vector of existing dates that should be excluded
#' @param format format for the date, see ?as.character
#' @return vector of dates from the period that don't exist in the give vector
getMissingDates <- function(start.date,
                            end.date,
                            existing.dates,
                            format = "%Y%m%d") {
  # Gets list of dates for which date range table is missing.
  days <- rep(1, end.date - start.date + 1)
  days.sequence <- seq_along(days)
  dates <- start.date - 1 + days.sequence
  dates <- as.character(dates, format)
  # existing dates come in "%Y%m%d" format
  existing.dates.asdate <- as.Date(existing.dates, "%Y%m%d")
  # we need to convert existing dates to format, which is in 'format' parameter
  existing.dates.formated <- as.character(existing.dates.asdate, format)
  # now we can compare dates, as they are in the same format
  res <- setdiff(dates, existing.dates.formated)
  return(res)
}

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
#' @details `bqDeleteDataset()` - You can protect dataset from programmatic deletion by adding `delete:never` label (key:value) to it.
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

#' Creates SQL statement from the source file
#'
#' @export
#' @param file file with sql statement template
#' @param ... any parameters that will be used to fill in placeholders in the template with sprintf
#' @return SQL statement as a string
readSql <- function(file, ...) {
  sql <-  paste(readLines(file), collapse = "\n")

  if (length(list(...)) > 0) {
    # template requires parameters.
    sql <- sprintf(sql, ...)
  } else {
    # template does not have parameteres.
    sql <- sql
  }

  return(sql)
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


#' @rdname bqTable
#'
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
                            dataset = bqDefaultDataset()) {
  bqAuth()

  if (!bqTableExists(table = table, dataset = dataset)) {
    tbl <- bigrquery::bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )

    if (partition) {
      bigrquery::bq_table_create(
        tbl,
        fields = read_json(schema.file),
        time_partitioning = list(type = "DAY")
      )
    } else {
      bigrquery::bq_table_create(
        tbl,
        fields = read_json(schema.file)
      )
    }
  }
  else {
    warning(paste0("Table already exists: [", dataset, ".", table, "]"))
  }
}

#' @export
#' @rdname bqTable
bqTableSchema <- function(table, dataset = bqDefaultDataset()) {
  bq_table_fields(
    bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )
  )
}


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

  sql <-  paste(readLines(file), collapse = "\n")
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

  params <- named_arguments(args, args.reserved)

  if (!use.legacy.sql) {
    assert_that(
      !(length(params) > 0L & noname_items_count(args) > 0L),
      msg = "Don't mix named and anonymous parameters in the call."
    )
  }

  if (length(list(...)) > 0) {
    # template requires parameters.
    sql <- sprintf(sql, ...)
  } else {
    # template does not have parameteres.
    sql <- sql
  }


  if (!use.legacy.sql & length(params) > 0) {
    cat("parameters applied to the template: \n")
    print(jsonlite::toJSON(params, auto_unbox = TRUE))
  } else {
    params = NULL
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

#' Returns subset of arguments where name is set excluding reserved names
#'
#' @noRd
named_arguments <- function(args, reserved) {
  args.names <- names(args)
  args.names <- args.names[nchar(args.names) > 0 & !(args.names %in% reserved)]
  args[args.names]
}

#' Counts number of items in the list that don't have names
#'
#' @noRd
noname_items_count <- function(x) {
  sum(nchar(names(x)) == 0)
}


bqDatasetLabel <- function(datasets, dataset) {
  labels <- names(datasets)
  names(labels) <- datasets
  return(labels[as.character(dataset)])
}

#' Creates partition table for a given sql
#'
#' @description
#' Parameters that will be passed to SQL for the placeholders:
#'
#' %1$s - the name of the BigQuery dataset
#' %2$s - the date (YYYYMMDD) of the partition
#'
#' @export
#' @param table name of the destination table
#' @param datasets list of Google Analytics properties to populate table for
#' @param sql sql to use a source of the data
#' @param file if sql is not provided it will be read from the file
#' @param existing.dates dates that should be skipped
#' @param missing.dates dates calculation for which will be enforced
#' @param priority sets priority of job execution to INTERACTIVE or BATCH
bqCreatePartitionTable <- function(table,
                                   datasets,
                                   sql = NULL,
                                   file = NULL,
                                   existing.dates = NULL,
                                   missing.dates = NULL,
                                   priority = "INTERACTIVE") {

  assert_that(
    xor(is.null(sql), is.null(file)),
    msg = "Either sql or file must be provided"
  )

  bqAuth()

  if (missing(sql)) {
    # Build SQL from code in the file.
    sql <- readLines(file)
  }

  if (missing(existing.dates)) {
    existing.dates <- bqExistingPartitionDates(table)
  }

  if (missing(missing.dates)) {
    missing.dates <- getMissingDates(
      bqStartDate(),
      bqEndDate(),
      existing.dates
    )
  }

  jobs <-
    lapply(missing.dates, function(d) {
      # Create partition for every missing date.
      destination.partition <- bqPartitionName(table, d)
      print(paste0("Partition name: ", destination.partition))


      sql.list <- lapply(datasets, function(p) {
        # Replace placeholders in sql template.
        sql.exec <- sprintf(sql, p, d, bqDatasetLabel(datasets, p))
        paste(sql.exec, collapse = "\n")
      })

      bqCreateTable(
        sql = bqCombineQueries(sql.list, TRUE),
        table =  destination.partition,
        priority = priority,
        write.disposition = "WRITE_TRUNCATE"
      )
    })

  bqWait(jobs, priority)
}

#' Combines list of queries into a single query
#'
#' @noRd
#' @param sql list of queries that will be combined into one
#' @param use.legacy.sql defines which flavour to use
bqCombineQueries <- function(sql, use.legacy.sql = bqUseLegacySql()) {
  if (use.legacy.sql) {
    sql.list <- lapply(sql, function(x) {
      paste0("(", x, ")")
    })
    paste0("SELECT * FROM\n", paste0(sql.list, collapse = ",\n"))
  } else {
    paste0(sql, collapse = "\n UNION ALL \n")
  }
}

#' Inserts data into BigQuery table
#'
#' @export
#' @param table name of the target table
#' @param data data to be inserted
#' @param dataset name of the destination dataset
#' @param append specifies if data should be appended or truncated
#' @param fields list of fields with names and types (as `bq_fields`)
#' @return results of execution
bqInsertData <- function(table,
                         data,
                         dataset = bqDefaultDataset(),
                         append = TRUE,
                         fields = NULL) {

  assert_that(
    nchar(dataset) > 0,
    msg = "Set dataset parameter or BIGQUERY_DATASET env var."
  )

  if (missing(fields) & ncol(data) > 0) {
    colnames(data) <- conformHeader(colnames(data), '_')
    fields <- as_bq_fields(data)
  }

  write.disposition <- ifelse(append, "WRITE_APPEND", "WRITE_TRUNCATE")
  rows <- nrow(data)

  if (rows > 0) {
    bqAuth()

    tbl <- bigrquery::bq_table(
      project = bqDefaultProject(),
      dataset = dataset,
      table = table
    )

    job <- bigrquery::bq_perform_upload(
      x = tbl,
      values = data,
      fields = fields,
      write_disposition = write.disposition,
      create_disposition = "CREATE_IF_NEEDED"
    )

    res <- bq_job_wait(job)

    return(res)
  } else {
    return(NULL)
  }
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


#' Creates partition name by combining table and partition date.
#'
#' @export
#' @param table Name of the table
#' @param date Partition date
#' @return Full partition table name
bqPartitionName <- function(table, date) {
  partition.time <- gsub("-", "", date)
  res <- paste0(table, "$", partition.time)
  return(res)
}

#' Deletes a specific partition of a partition table.
#'
#' @export
#' @param table Name of the table
#' @param date Partition date
bqDeletePartition <- function(table, date) {
  name <- bqPartitionName(table, date)
  bqDeleteTable(name)
}

#' Inserts data table into a specific partition of a partition table.
#'
#' @export
#' @param table Name of the table
#' @param date Partition date
#' @param data Data table to insert
#' @param append Append to the partition if TRUE else overwrite
bqInsertPartition <- function(table, date, data, append = FALSE) {
  target.partition <- bqPartitionName(table, date)

  bqInsertData(
    table = target.partition,
    data = data,
    append = append
  )
}


#' Functions to transforms partitioned data form one table to another
#'
#' @description `bqTransformPartition` creates new partitions for the missing dates
#' @rdname bqPartition
#' @export
#' @param table destination partition table where results of the query will be saved
#' @param file path to the sql file that will be used for the transformation
#' @param ...  parameters that will be passed via `sprintf` to build dynamic SQL.
#'    partition date will be always passed first in format `yyyymmdd`
#'    followed by arguments in `...`
#' @inheritParams bqCreateTable
bqTransformPartition <- function(table,
                                 file,
                                 ...,
                                 priority = "INTERACTIVE",
                                 use.legacy.sql = bqUseLegacySql()) {
  existing.dates <- bqExistingPartitionDates(table)
  start.date <- bqStartDate(unset = "2017-01-01")
  end.date <- bqEndDate()

  missing.dates <- getMissingDates(
    start.date,
    end.date,
    existing.dates,
    "%Y-%m-%d"
  )

  jobs <- lapply(missing.dates, function(d) {
    partition <- gsub("-", "", d)
    destination.partition <- paste0(table, "$", partition)
    print(destination.partition)
    sql.exec <- readSql(file, d, ...)

    bqCreateTable(
      sql.exec,
      table = destination.partition,
      write.disposition = "WRITE_TRUNCATE",
      priority = priority,
      use.legacy.sql = use.legacy.sql
    )
  })

  bqWait(jobs, priority)
}

#' @description `bqRefreshPartitionData` updates existing partitions in the target table
#'
#' @rdname bqPartition
#' @export
bqRefreshPartitionData <- function(table,
                                   file,
                                   ...,
                                   priority = "BATCH",
                                   use.legacy.sql = bqUseLegacySql()) {
  existing.dates <- bqExistingPartitionDates(table)

  jobs <- lapply(existing.dates, function(d) {
    partition <- gsub("-", "", d)
    destination.partition <- paste0(table, "$", partition)
    sql <- readSql(file, d, ...)

    bqCreateTable(
      sql = sql,
      table = destination.partition,
      write.disposition = "WRITE_TRUNCATE",
      priority = priority,
      use.legacy.sql = use.legacy.sql
    )
  })
  bqWait(jobs, priority)
}

#' Waits for jobs provided in the list if priority is BATCH
#' @noRd
bqWait <- function(jobs, priority) {
  if (priority == "BATCH") {
    # Wait for all the jobs that were submitted
    jobs <- lapply(jobs, function(job) {
      bq_job_wait(job)
    })
  }
  invisible(jobs)
}

bqStartDate <- function(unset = "2016-01-01") {
  date <- Sys.getenv("BIGQUERY_START_DATE")
  date <- ifelse(nchar(date) > 0, date, unset)
  as.Date(date)
}

bqEndDate <- function(unset = as.character(Sys.Date() - 1)) {
  date <- Sys.getenv("BIGQUERY_END_DATE")
  date <- ifelse(nchar(date) > 0, date, unset)
  as.Date(date)
}

getInString <- function(x) {
  paste0("'", paste(x, collapse = "', '"), "'")
}

#' Returns a case clause based on binning the input vector
#' to n+1 bins
#'
#' @export
#' @param field field name to be used for the binning
#' @param limits vector of separator values
#' @param alias resulting field name for the case
#' @return case clause to be included in a SQL statement
bqVectorToCase <- function(field, limits, alias = field) {
  assert_that(
    length(limits) > 0,
    is.numeric(limits),
    is.character(field)
  )

  limits <- c(-Inf, limits, Inf)

  case.body <- sapply(1:(length(limits) - 1), function(i) {
    paste0(
      "WHEN (",
      case_condition(field, limits[i], limits[i + 1]),
      ") THEN '",
      case_label(i, limits[i], limits[i + 1])
    )
  })
  case.body <- paste0(case.body, collapse = "")

  paste0("CASE ", case.body, "END AS ", alias)
}

case_condition <- function(field, low, high) {
  low.limit <- paste0(field, " > ", low)
  high.limit <- paste0(field, " <= ", high)
  if (is.infinite(low)) {
    return(high.limit)
  }
  if (is.infinite(high)) {
    return(low.limit)
  }
  paste0(
    low.limit, " AND ", high.limit
  )
}

case_label <- function(index, low, high) {
  paste0(LETTERS[index], ") (", low, ", ", high, case_right_bracket(high), "' ")
}

case_right_bracket <- function(high) {
  ifelse(is.infinite(high), ")", "]")
}

#' Save file inferred from data.table to JSON file
#'
#' @export
#' @rdname bqSchema
#' @param dt data.table, table name or bq_fields from which schema will be extracted into JSON
#' @param file path to the schema file
bqSaveSchema <- function(dt, file) {
  dt.schema <- bqExtractSchema(dt)
  write(dt.schema, file)
}

#' Extract schema as JSON from data.table
#'
#' @rdname bqSchema
#' @export
bqExtractSchema <- function(dt) {
  if (is.data.frame(dt)) {
    dt.copy <- copy(head(dt))
    colnames(dt.copy) <- conformHeader(colnames(dt.copy), "_")
    fields <- as_bq_fields(dt.copy)
  }
  else if (is.string(dt)) {
    fields <- bqTableSchema(dt)
  }
  else if (class(dt) == "bq_fields") {
    fields <- dt
  }
  jsonlite::toJSON(bqJsonFields(fields), pretty = TRUE)
}

bqJsonField <- function(x) {
  res <- list(
    name = jsonlite::unbox(x$name),
    type = jsonlite::unbox(x$type),
    mode = jsonlite::unbox(x$mode)
   )
  if (!is.null(x$fields) & length(x$fields) > 0) {
    res$fields <- bqJsonFields(x$fields)
  }
  res
}

bqJsonFields <- function(x) {
  lapply(x, bqJsonField)
}
