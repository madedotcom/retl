#' @import utils
#' @import bigrquery
#' @import stringr
#' @import assertthat

# Required environment variables to use BigQuery helper functions:
# BIGQUERY_PROJECT - name of the project in BigQuery.
# BIGQUERY_DATASET - name of the default dataset in BigQuery.
# BIGQUERY_ACCESS_TOKEN_PATH - path to the json token file.

library(bigrquery)
library(stringr)
library(jsonlite)
library(assertthat)


#' Wrapper for the set_service_token that uses BIGQUERY_ACCESS_TOKEN_PATH env var
#'  as default value for the secret token location.
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
    return(res$partition_id)
  }
  else {
    return(character())
  }
}

#' @rdname bqExistingPartitionDates
#' @export
getExistingPartitionDates <- function(table) {
  .Deprecated("bqExistingPartitionDates")
  bqExistingPartitionDates(table)
}

#' Generates sql for extraction of existing partition date
#' @noRd
bqPartitionDatesSql <- function(table) {
  if (bqUseLegacySql()) {
    paste0("SELECT partition_id from [", table, "$__PARTITIONS_SUMMARY__];")
  } else {
    paste0("SELECT
           FORMAT_DATE('%Y%m%d', DATE(_PARTITIONTIME)) as partition_id from `", table, "`
           GROUP BY 1;")
  }
}

#' Gest the value from the corresponding environment variable as boolean
#' Determins which flavour of sql should be used by default.
#' @noRd
bqUseLegacySql <- function() {
  Sys.getenv("BIGQUERY_LEGACY_SQL", unset = "TRUE") == "TRUE"
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

#' Creates partition in specified table in BigQuery.
#
#' @param table name of the new table
#' @param sql source for the table as string
#' @param file source for the tabel as file
#' @param existing.dates Vector of dates that already exist in the target table. If provided, masked tables for these dates will be exlcuded
#' @param missing.dates Vector of dates that should be created. If provided data for those dates will be added or re-created.
#' @note sql or file must be provided
createPartitionTable <- function(table, sql = NULL, file = NULL, existing.dates = NULL, missing.dates = NULL) {
  .Deprecated("bqCreatePartitionTable")
}

#' Creates range teable in BigQuery.
#'
#' @param table name of the new table
#' @param sql source for the table as string
#' @param file source for the tabel as file
#' @note sql or file must be provided
createRangeTable <- function(table, sql = NULL, file = NULL) {

  if (missing(sql)) {
    # Build SQL from code in the file.
    sql <- paste(readLines(file), collapse = "\n")
  }

  start.date <- as.Date("2016-01-08")
  end.date <- Sys.Date() - 1

  existing.dates <- bqExistingPartitionDates(table)
  missing.dates <- getMissingDates(start.date, end.date, existing.dates)

  lapply(missing.dates, function(d) {
    print(d)
    sql.exec <- sprintf(sql, d)
    bqCreateTable(
      sql = sql.exec,
      table = paste0(table, d),
      write_disposition = "WRITE_TRUNCATE"
    )
  })
}

#' Gets existing dates from wildcard tables in BigQuery
#'
#' @export
#' @param dataset name of a dataset
#' @param table.prefix name of the table before the wildcard
#' @return string vector of dates
getExistingDates <- function(dataset, table.prefix) {
  bqAuth()
  tables <- list_tables(
    project = bqDefaultProject(),
    dataset,
    10000
  )
  matches <- tables[grepl(table.prefix, tables)]
  res <- str_extract(matches,"\\d{8}")
  return(res)
}

#' Gets dates that are missing from the date range for a give list of existing dates
#'
#' @export
#' @param start.date begining of the period
#' @param end.date end of the period
#' @param existing.dates vector of existing dates that should be exlcuded
#' @param format format for the date, see ?as.character
#' @return vector of dates from the period that don't exist in the give vector
getMissingDates <- function(start.date, end.date, existing.dates, format = "%Y%m%d") {
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

#' Checks if dataset exists in the project
#'
#' @rdname bqDataset
#'
#' @export
#' @param dataset name of the dataset
#' @param project name of the project
#' @return `bqDatasetExists` TRUE if dataset exists
bqDatasetExists <- function(dataset = bqDefaultDataset(), project = bqDefaultProject()) {
  bqAuth()
  ds <- bq_dataset(project, dataset)
  bq_dataset_exists(ds)
}

#' @rdname bqDataset
#'
#' @export
bqCreateDataset <- function(dataset = bqDefaultDataset(), project = bqDefaultProject()) {
  bqAuth()
  bq_dataset_create(
    bq_dataset(
      project = project,
      dataset = dataset
    )
  )
}

#' @rdname bqDataset
#'
#' @export
bqDeleteDataset <- function(dataset = bqDefaultDataset(), project = bqDefaultProject()) {
  bqAuth()
  bq_dataset_delete(
    bq_dataset(
      project = project,
      dataset = dataset
    )
  )
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
  assert_that(nchar(dataset) > 0, msg = "Set dataset parameter or BIGQUERY_DATASET env var.")

  bqAuth()
  bt <- bq_table(
    project = bqDefaultProject(),
    dataset = dataset,
    table = table
  )
  bq_table_delete(bt)
}

#' Creates SQL statment from the source file
#'
#' @export
#' @param file file with sql statment template
#' @param ... any parameters that will be used to fill in placeholders in the template with sprintf
#' @return SQL statment as a string
readSql <- function(file, ...) {
  sql <-  paste(readLines(file), collapse = "\n")

  if (length(list(...)) > 0) { # template requires parameters.
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
#' @param sql SQL statement to use a source for a new table
#' @param table name of a table to be created
#' @param dataset name of the destination dataset
#' @param write_disposition defines whether records will be appended
#' @param priority defines whether query will have interactive or batch priority
#' @return results of the exectuion as returned by bigrquery::query_exec
bqCreateTable <- function(sql,
                          table,
                          dataset = bqDefaultDataset(),
                          write_disposition = "WRITE_APPEND",
                          priority = "INTERACTIVE") {
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
    write_disposition = write_disposition,
    use_legacy_sql = bqUseLegacySql(),
    priority = priority
  )
  bq_job_wait(job)
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

  if (!bqTableExists(table)) {
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
#' Placeholders in template are replaced with values provided in ellipsis parameter with sprintf.
#'
#' @name bqQuery
NULL

#' Gets data for a given SQL statement or file that contains SQL
#'
#' @rdname bqQuery
#'
#' @export
#' @param sql sql statement
#' @param file file with sql statment
#' @param ... any parameters that will be used to fill in placeholders with sprintf
#' @return results of execution as data.frame
bqGetData <- function(sql = NULL, file = NULL, ...) {
  # Wrapper function to load data from BigQuery.

  if (!missing(file)) { # Gets sql from file.
    return(bqExecuteFile(file, ...))
  }
  else {
    return(bqExecuteSql(sql, ...))
  }
}

#' Gets data for a given file that contains SQL statement
#'
#' @rdname bqQuery
#'
#' @export
#' @param file file with sql statment
#' @param ... any parameters that will be used to fill in placeholders with sprintf
#' @return results of execution as data.frame
bqExecuteFile <- function(file, ...) {
  # Function to load data from BigQuery using file with SQL.

  sql <-  paste(readLines(file), collapse = "\n")
  res <- bqExecuteSql(sql, ...)
  return(data.table(res))
}

#' @rdname bqQuery
#'
#' @export
#' @param query template of the sql statement
bqExecuteQuery <- function(query, ...) {
  bqExecuteSql(query, ...)
}

#' Gets data for a given SQL statement
#'
#' @rdname bqQuery
#'
#' @export
#' @param sql string with sql statment
#' @param ... any parameters that will be used to fill in placeholders with sprintf
#' @return results of execution as data.frame
bqExecuteSql <- function(sql, ...) {
  if (length(list(...)) > 0) { # template requires parameters.
    sql <- sprintf(sql, ...)
  } else {
    # template does not have parameteres.
    sql <- sql
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
    use_legacy_sql = bqUseLegacySql()
  )
  data.table(bigrquery::bq_table_download(tb))
}

#' Gets the shop code from the GA properties vector.
#
#' @param ga.properties named vector of Google Analytics properties.
#' Names are ISO2 codes of the country.
#' @param  property number of a property in Google Analytics.
#' @return gets site code for a given propertiy code
gaGetShop <- function(ga.properties, property) {
  .Deprecated(msg = "GA related logic should be moved to another package")
  shops <- names(ga.properties)
  names(shops) <- ga.properties
  return(shops[as.character(property)])
}

bqDatasetLabel <- function(datasets, dataset) {
  labels <- names(datasets)
  names(labels) <- datasets
  return(labels[as.character(dataset)])
}

#' Creates partition table for a given sql
#'
#' @export
#' @param table name of the destination table
#' @param datasets list of Google Analytics properties to populate table for
#' @param sql sql to use a source of the data
#' @param file if sql is not provided it will be read from the file
#' @param existing.dates dates that should be skipped
#' @param missing.dates dates calculation for which will be enforced
bqCreatePartitionTable <- function(table, datasets,
                                   sql = NULL, file = NULL,
                                   existing.dates = NULL,
                                   missing.dates = NULL) {

  assert_that(xor(is.null(sql), is.null(file)), msg = "Either sql or file must be provided")

  bqAuth()

  if (missing(sql)) {
    # Build SQL from code in the file.
    sql <- paste(readLines(file), collapse = "\n")
  }

  if (missing(existing.dates)) {
    existing.dates <- bqExistingPartitionDates(table)
  }

  if (missing(missing.dates)) {
    missing.dates <- getMissingDates(bqStartDate(), bqEndDate(), existing.dates)
  }

  res <-
    lapply(missing.dates, function(d) { # Create partition for every missing date.
      destination.partition <- bqPartitionName(table, d)
      print(paste0("Partition name: ", destination.partition))

      bqDeleteTable(destination.partition)

      lapply(datasets, function(p) {
        sql.exec <- sprintf(sql, p, d, bqDatasetLabel(datasets, p)) # Replace placeholder in sql template.
        bqCreateTable(
          sql = sql.exec,
          table =  destination.partition
        )
      })
    })

  invisible(res)
}

#' Inserts data into BigQuery table
#'
#' @export
#' @param table name of the target table
#' @param data data to be inserted
#' @param dataset name of the destination dataset
#' @param append specifies if data should be appended or truncated
#' @param job.name name of the ETL job that will be written to the metadata execution log
#' @param increment.field specifies field that is used for incremental data loads
#' @return results of execution
bqInsertData <- function(table,
                         data,
                         dataset = bqDefaultDataset(),
                         append = TRUE,
                         job.name = NULL, increment.field = NULL) {
  assert_that(nchar(dataset) > 0, msg = "Set dataset parameter or BIGQUERY_DATASET env var.")

  assert_that(!xor(is.null(job.name), is.null(increment.field)),
              msg = "increment.field and job.name arguments are both required if one is provided.")

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
      write_disposition = write.disposition,
      create_disposition = "CREATE_IF_NEEDED"
    )

    res <- bq_job_wait(job)

    if (!is.null(job.name)) {
      # Log metadata of the execution with number of rows and increment value
      increment.value <- as.integer(max(data[, get(increment.field)]))
      etlLogExecution(job.name, increment.value, rows)
    }
    return(res)
  } else {
    return(NULL)
  }
}

#' Gets list of the column names for a given table
#'
#' @export
#' @param table name of the table
#' @return columns of a table
bqGetColumnNames <- function(table) {
  bqAuth()

  info <- get_table(project = bqDefaultProject(),
                    dataset = bqDefaultDataset(),
                    table)

  # Unlist all the schema data and keep only the name fields
  # remove the naming and then return the vector with only
  # the names
  fields <- unlist(info$schema$fields)
  fields <- fields[names(fields) == "name"]
  names(fields) <- NULL

  return(fields)
}

#' Copies table in BigQuery
#'
#' @export
#' @param from name of the source table
#' @param to name of the desitnation table
#' @param override defines if command will override existing table if it is not empty.
#' @return TRUE if the table has been succesfully copied
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

  write_disposition <- ifelse(override, "WRITE_TRUNCATE", "WRITE_EMPTY")
  bq_table_copy(
    x = src,
    dest = dest,
    write_disposition = write_disposition
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
  partition.time <- gsub("-","", date)
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

  bqInsertData(table = target.partition,
               data = data,
               append = append)
}

#' Functions to transforms partitioned data into a partitioned table
#'
#' @description `bqTransformPartition` creates new partitions for the missing dates
#' @rdname bqPartition
#' @export
#' @param table destination partition table where resutls of the query will be saved
#' @param file path to the sql file that will be used for the transformation
#' @param ...  parameters that will be passed via `sprintf` to build dynamic SQL.
#'    partition date will be always passed first in format `yyyymmdd`
#'    followed by arguments in `...`
bqTransformPartition <- function(table, file, ...) {
  existing.dates <- bqExistingPartitionDates(table)
  start.date <- bqStartDate(unset = "2017-01-01")
  end.date <- bqEndDate()

  missing.dates <- getMissingDates(
    start.date,
    end.date,
    existing.dates,
    "%Y-%m-%d"
    )

  lapply(missing.dates, function(d) {
    partition <- gsub("-", "", d)
    destination.partition <- paste0(table, "$", partition)
    print(destination.partition)
    sql.exec <- readSql(file, d, ...)

    bqCreateTable(
      sql.exec,
      table = destination.partition,
      write_disposition = "WRITE_TRUNCATE")
  })
}

#' @description `bqRefreshPartitionData` updates existing partitions in the target table
#'
#' @rdname bqPartition
#' @param priority sets priority of the execution, BATCH or INTERACTIVE
#' @export
bqRefreshPartitionData <- function(table, file, ..., priority = "BATCH") {
  existing.dates <- bqExistingPartitionDates(table)
  lapply(existing.dates, function(d) {
    partition <- gsub("-", "", d)
    destination.partition <- paste0(table, "$", partition)
    sql <- readSql(file, d, ...)

    bqCreateTable(
      sql = sql,
      table = destination.partition,
      write_disposition = "WRITE_TRUNCATE",
      priority = priority
    )
  })
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
#' @param limits vector of seperator values
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

