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
  if (!is_set_access_cred()) {
    token.file = Sys.getenv("BIGQUERY_ACCESS_TOKEN_PATH")
    assert_that(token.file != "", file.exists(token.file))
    set_service_token(token.file)
  }
}

#' Gets existing dates for date partitioned table in BigQuery
#'
#' @export
#' @param table name of a table
#' @return string vector of dates
getExistingPartitionDates <- function(table) {

  if (!bqTableExists(table)) {
    return(character())
  }

  sql <- paste0("SELECT partition_id from [", table, "$__PARTITIONS_SUMMARY__];")
  res <- bqExecuteSql(sql)
  if (nrow(res) > 0) {
    return(res$partition_id)
  }
  else {
    return(character())
  }
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

  # StartDate
  start.date <- as.Date("2016-01-08")
  #EndDate
  end.date <- Sys.Date() - 1


  dataset <- Sys.getenv("BIGQUERY_DATASET")

  existing.dates <- getExistingPartitionDates(table)
  missing.dates <- getMissingDates(start.date, end.date, existing.dates)

  lapply(missing.dates, function(d) {
    print(d)
    sql.exec <- sprintf(sql, d)
    query_exec(query = sql.exec,
               project = Sys.getenv("BIGQUERY_PROJECT"),
               destination_table = paste0(dataset, ".", table, d),
               max_pages = 1,
               page_size = 1,
               create_disposition = "CREATE_IF_NEEDED",
               write_disposition = "WRITE_TRUNCATE")

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
  tables <- list_tables(project = Sys.getenv("BIGQUERY_PROJECT"), dataset, 10000)
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
  res <- setdiff(dates, existing.dates)
  return(res)
}

#' Checks if table exists
#'
#' @export
#' @param table name of the table
#' @return TRUE if table exists
bqTableExists <- function(table) {
  bqAuth()
  res <- exists_table(
    project = Sys.getenv("BIGQUERY_PROJECT"),
    dataset = Sys.getenv("BIGQUERY_DATASET"),
    table = table
  )
  return(res)
}

#' Deletes table
#'
#' @export
#' @param table name of the table
#' @param dataset name of the dataset
#' @return results of the execution from bigrquery::delete_table
bqDeleteTable <- function(table,
                          dataset = Sys.getenv("BIGQUERY_DATASET")) {
  assert_that(nchar(dataset) > 0, msg = "Set dataset parameter or BIGQUERY_DATASET env var.")

  bqAuth()
  res <- delete_table(project = Sys.getenv("BIGQUERY_PROJECT"),
                      dataset = dataset,
                      table = table)
  return(res)
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
#' @return results of the exectuion as returned by bigrquery::query_exec
bqCreateTable <- function(sql,
                          table,
                          dataset = Sys.getenv("BIGQUERY_DATASET"),
                          write_disposition = "WRITE_APPEND" ) {
  bqAuth()
  use.legacy.sql <- Sys.getenv("BIGQUERY_LEGACY_SQL", unset = "TRUE") == "TRUE"

  # Creates table from the given SQL.
  res <- query_exec(
    query = sql,
    project = Sys.getenv("BIGQUERY_PROJECT"),
    default_dataset = dataset,
    destination_table = paste0(dataset, ".", table),
    max_pages = 1,
    page_size = 1,
    create_disposition = "CREATE_IF_NEEDED",
    write_disposition = write_disposition,
    use_legacy_sql = use.legacy.sql
  )
  return(res)
}

#' Creates table from the json schema file.
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
                            dataset = Sys.getenv("BIGQUERY_DATASET")) {
  bqAuth()
  insert_table(
    project = Sys.getenv("BIGQUERY_PROJECT"),
    dataset = dataset,
    table = table,
    schema = read_json(schema.file),
    partition = partition
  )
}

#' Gets data for a given SQL statement or file that contains SQL
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

#' Gets data for a given SQL statement
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

  use.legacy.sql <- Sys.getenv("BIGQUERY_LEGACY_SQL", unset = "TRUE") == "TRUE"

  bqAuth()
  res <- query_exec(sql,
                    project = Sys.getenv("BIGQUERY_PROJECT"),
                    default_dataset = Sys.getenv("BIGQUERY_DATASET"),
                    max_pages = Inf,
                    use_legacy_sql = use.legacy.sql)
  return(data.table(res))
}

#' Gets the shop code from the GA properties vector.
#
#' @param ga.properties named vector of Google Analytics properties.
#' Names are ISO2 codes of the country.
#' @param  property number of a property in Google Analytics.
#' @return gets site code for a given propertiy code
gaGetShop <- function(ga.properties, property) {
  shops <- names(ga.properties)
  names(shops) <- ga.properties
  return(shops[as.character(property)])
}

#' Creates partition table for a given sql
#'
#' @export
#' @param table name of the destination table
#' @param ga.properties list of Google Analytics properties to populate table for
#' @param sql sql to use a source of the data
#' @param file if sql is not provided it will be read from the file
#' @param existing.dates dates that should be skipped
#' @param missing.dates dates calculation for which will be enforced
bqCreatePartitionTable <- function(table, ga.properties,
                                   sql = NULL, file = NULL,
                                   existing.dates = NULL,
                                   missing.dates = NULL) {

  assert_that(xor(is.null(sql), is.null(file)), msg = "Either sql or file must be provided")

  bqAuth()

  if (missing(sql)) {
    # Build SQL from code in the file.
    sql <- paste(readLines(file), collapse = "\n")
  }

  # StartDate - start of Custom Dimension for UserID
  start.date <- as.Date(Sys.getenv("BIGQUERY_START_DATE"))
  #EndDate
  if (Sys.getenv("BIGQUERY_END_DATE") == "") {
    end.date <- Sys.Date() - 1
  } else {
    end.date <- as.Date(Sys.getenv("BIGQUERY_END_DATE"))
  }

  if (missing(existing.dates)) {
    existing.dates <- getExistingPartitionDates(table)
  }

  if (missing(missing.dates)) {
    missing.dates <- getMissingDates(start.date, end.date, existing.dates)
  }

  res <-
    lapply(missing.dates, function(d) { # Create partition for every missing date.
      destination.partition <- bqPartitionName(table, d)
      print(paste0("Partition name: ", destination.partition))

      bqDeleteTable(destination.partition)

      lapply(ga.properties, function(p) {
        sql.exec <- sprintf(sql, p, d, gaGetShop(ga.properties, p)) # Replace placeholder in sql template.
        query_exec(query = sql.exec,
                   project = Sys.getenv("BIGQUERY_PROJECT"),
                   default_dataset = Sys.getenv("BIGQUERY_DATASET"),
                   destination_table = paste0(Sys.getenv("BIGQUERY_DATASET"), ".", destination.partition),
                   max_pages = 1,
                   page_size = 1,
                   create_disposition = "CREATE_IF_NEEDED",
                   write_disposition = "WRITE_APPEND")
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
                         dataset = Sys.getenv("BIGQUERY_DATASET"),
                         append = TRUE,
                         job.name = NULL, increment.field = NULL) {
  assert_that(nchar(dataset) > 0, msg = "Set dataset parameter or BIGQUERY_DATASET env var.")

  assert_that(!xor(is.null(job.name), is.null(increment.field)),
    msg = "increment.field and job.name arguments are both required if one is provided.")

  write.disposition <- ifelse(append, "WRITE_APPEND", "WRITE_TRUNCATE")
  rows <- nrow(data)

  if (rows > 0) {
    bqAuth()
    job <- insert_upload_job(project = Sys.getenv("BIGQUERY_PROJECT"),
                             dataset = dataset,
                             table,
                             data,
                             write_disposition = write.disposition,
                             create_disposition = "CREATE_IF_NEEDED")

    res <- wait_for(job)

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

  info <- get_table(project = Sys.getenv("BIGQUERY_PROJECT"),
                    dataset = Sys.getenv("BIGQUERY_DATASET"),
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
#' @return TRUE if the table has been succesfully copied
bqCopyTable <- function(from, to) {
  bqAuth()

  src <- list(
    project_id = Sys.getenv("BIGQUERY_PROJECT"),
    dataset_id = Sys.getenv("BIGQUERY_DATASET"),
    table_id = from
  )

  dest <- list(
    project_id = Sys.getenv("BIGQUERY_PROJECT"),
    dataset_id = Sys.getenv("BIGQUERY_DATASET"),
    table_id = to
  )

  job <- copy_table(
    src = src,
    dest = dest,
    write_disposition = "WRITE_TRUNCATE",
    project = Sys.getenv("BIGQUERY_PROJECT")
  )

  wait_for(job)
  print(job)
  return(bqTableExists(to))
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
  res <- "CASE "
  mainBody <- paste0("WHEN (", field, " <= ", limits[1], ") THEN '", LETTERS[1] ,") (-Inf, ", limits[1], "]' ")

  if (length(limits) > 1) {
    for (i in 2:(length(limits)) - 1) {
      tmp <- paste0("WHEN (", field, " > ", limits[i], " AND ",
                    field, " <= ", limits[i + 1], ") THEN '", LETTERS[i + 1] ,") (", limits[i], ", ", limits[i + 1], "]' ")
      mainBody <- paste0(mainBody, tmp)
    }
  }

  mainBody <- paste0(mainBody, "WHEN (", field, " > ", limits[length(limits)], ") THEN '", LETTERS[length(limits) + 1] ,") (", limits[length(limits)], ", +Inf)' ")

  res <- paste0(res, mainBody, "END AS ", alias)
  return(res)
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
bqInsertPartition <- function(table, date, data, append) {
  target.partition <- bqPartitionName(table, date)

  bqInsertData(table = target.partition,
               data = data,
               append = append)
}

#' Transforms data from one partition table to another partition table
#'
#' @param table destination partition table where resutls of the query will be saved
bqTransformPartition <- function(table, file, ...) {
    existing.dates <- getExistingPartitionDates(table)
    existing.dates <- as.Date(existing.dates, "%Y%m%d")
    existing.dates <- as.character(existing.dates)
    start.date <- as.Date(Sys.getenv("BIGQUERY_START_DATE", unset = "2017-01-01"))
    end.date <- as.Date(Sys.getenv("BIGQUERY_END_DATE", Sys.Date() - 1))
    missing.dates <- getMissingDates(start.date, end.date, existing.dates, "%Y-%m-%d")
    lapply(missing.dates, function(d) {
        partition <- gsub("-", "", d)
        
        destination.partition <- paste0(table, "$", partition)
        delete_table(project = Sys.getenv("BIGQUERY_PROJECT"),
        dataset = Sys.getenv("BIGQUERY_DATASET"),
        table = destination.partition)
        
        sql.exec <- readSql(file, d, ...)
        print(destination.partition)
        
        use.legacy.sql <- Sys.getenv("BIGQUERY_LEGACY_SQL", unset = "TRUE") == "TRUE"
        query_exec(query = sql.exec,
        project = Sys.getenv("BIGQUERY_PROJECT"),
        default_dataset = Sys.getenv("BIGQUERY_DATASET"),
        destination_table = paste0(Sys.getenv("BIGQUERY_DATASET"), ".", destination.partition),
        max_pages = 1,
        page_size = 1,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_TRUNCATE",
        use_legacy_sql = use.legacy.sql)
    })
}


getInString <- function(x) {
    paste0("'", paste(x, collapse = "', '"), "'")
}
