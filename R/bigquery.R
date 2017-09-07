#' @import utils
#' @import bigrquery
#' @import stringr

# Required environment variables to use BigQuery helper functions:
# BIGQUERY_PROJECT - name of the project in BigQuery.
# BIGQUERY_DATASET - name of the default dataset in BigQuery.

library(bigrquery)
library(stringr)

#' Gets existing dates for date partitioned table in BigQuery
#'
#' @export
#' @param table name of a table
#' @return string vector of dates
getExistingPartitionDates <- function(table) {
  project <- Sys.getenv("BIGQUERY_PROJECT")
  dataset <- Sys.getenv("BIGQUERY_DATASET")

  if(!exists_table(project = project,
                   dataset = dataset,
                   table = table)) {
    return(NULL)
  }

  sql <- paste0("SELECT partition_id from [", dataset , ".", table, "$__PARTITIONS_SUMMARY__];")
  res <- query_exec(query = sql,
                    project = project)
  if(nrow(res) > 0) {
    return(res$partition_id)
  }
  else {
    return(NULL)
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

  if(missing(sql)) {
    # Build SQL from code in the file.
    sql <- paste(readLines(file), collapse="\n")
  }

  # StartDate
  start.date <- as.Date("2016-01-08")
  #EndDate
  end.date <- Sys.Date() - 1


  project <- Sys.getenv("BIGQUERY_PROJECT")
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
#' @param bq.dataset name of a dataset
#' @param table.prefix name of the table before the wildcard
#' @param bq.project name of the project
#' @return string vector of dates
getExistingDates <- function(bq.dataset, table.prefix, bq.project = Sys.getenv("BIGQUERY_PROJECT")) {
  # Gets list of dates for which date range table exists.
  tables <- list_tables(bq.project, bq.dataset, 10000)
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
#' @return vector of dates from the period that don't exist in the give vector
getMissingDates <- function(start.date, end.date, existing.dates) {
  # Gets list of dates for which date range table is missing.
  days <- rep(1, end.date - start.date + 1)
  days.sequence <- seq_along(days)
  dates <- start.date -1 + days.sequence
  dates <- as.character(dates, "%Y%m%d")
  res <- setdiff(dates, existing.dates)
  return (res)
}

#' Gets last id from a given field
#'
#' @export
#' @param bq.table name of the table
#' @param field name of the field
#' @param bq.project name of the project
#' @param bq.dataset name of the dataset
#' @return max value in a requested field
getLastID <- function(bq.table, field, bq.project = Sys.getenv("BIGQUERY_PROJECT"),
                                       bq.dataset =  Sys.getenv("BIGQUERY_DATASET")) {
  sql.tempalte <- "SELECT MAX(%1$s) as ID FROM [%2$s.%3$s]"
  sql <- sprintf(sql.tempalte, field, bq.dataset, bq.table)
  res <- query_exec(sql, project = bq.project)
  res <- head(res$ID, 1)
  if(is.na(res)) {
    res <- 0
  }
  return(res)
}

#' Checks if table exists
#'
#' @export
#' @param table.name name of the table
#' @return TRUE if table exists
bqTableExists <- function(table.name) {
  res <- exists_table(Sys.getenv("BIGQUERY_PROJECT"),
                      dataset = Sys.getenv("BIGQUERY_DATASET"),
                      table = table.name)
  return(res)
}

#' Deletes table
#'
#' @export
#' @param bq.table name of the table
#' @param bq.project name of the project
#' @param bq.dataset name of the dataset
#' @return results of the execution from bigrquery::delete_table
bqDeleteTable <- function(bq.table, bq.project = Sys.getenv("BIGQUERY_PROJECT"),
                                      bq.dataset = Sys.getenv("BIGQUERY_DATASET")) {
  res <- delete_table(project = bq.project, dataset = bq.dataset, table = bq.table)
  return(res)
}

#' Creates table using given sql
#'
#' @export
#' @param sql SQL statement to use a source for a new table
#' @param bq.table name of a table to be created
#' @param bq.project name of the destination project
#' @param bq.dataset name of the destination dataset
#' @param write_disposition defines whether records will be appended
#' @return results of the exectuion as returned by bigrquery::query_exec
bqCreateTable <- function(sql, bq.table, bq.project = Sys.getenv("BIGQUERY_PROJECT"),
                          bq.dataset = Sys.getenv("BIGQUERY_DATASET"),
                          write_disposition = "WRITE_APPEND" ) {
  # Creates table from the given SQL.
  res <- query_exec(query = sql,
                    project = bq.project, default_dataset = bq.dataset,
                    destination_table = paste0(bq.dataset, ".", bq.table),
                    max_pages = 1,
                    page_size = 1,
                    create_disposition = "CREATE_IF_NEEDED",
                    write_disposition = write_disposition)
  return(res)
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

  if(!missing(file)) { # Gets sql from file.
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

  sql <-  paste(readLines(file), collapse="\n")
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
  if(length(list(...)) > 0) { # template requires parameters.
    sql <- sprintf(sql, ...)
  } else { # template does not have parameteres.
    sql <- sql
  }

  res <- query_exec(sql,
                    project = Sys.getenv("BIGQUERY_PROJECT"),
                    default_dataset = Sys.getenv("BIGQUERY_DATASET"),
                    max_pages = Inf)
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
bqCreatePartitionTable <- function(table, ga.properties, sql = NULL, file = NULL, existing.dates = NULL, missing.dates = NULL) {
  # Creates partition in specified table in BigQuery.
  #
  # Parameters:
  #   table - name of the new table
  #   sql - source for the table as string.
  #   file - source for the tabel as file.
  # Note: sql or file must be provided.

  if(missing(sql)) {
    # Build SQL from code in the file.
    sql <- paste(readLines(file), collapse="\n")
  }

  # StartDate - start of Custom Dimension for UserID
  start.date <- as.Date(Sys.getenv("BIGQUERY_START_DATE"))
  #EndDate
  if (Sys.getenv("BIGQUERY_END_DATE") == ""){
    end.date <- Sys.Date() - 1
  } else {
    end.date <- as.Date(Sys.getenv("BIGQUERY_END_DATE"))
  }

  if(missing(existing.dates)) {
    existing.dates <- getExistingPartitionDates(table)
  }

  if(missing(missing.dates)) {
    missing.dates <- getMissingDates(start.date, end.date, existing.dates)
  }

  res <-
    lapply(missing.dates, function(d) { # Create partition for every missing date.
      destination.partition <- paste0(table, "$", d)
      print(paste0("Partition name: ", destination.partition))

      delete_table(project = Sys.getenv("BIGQUERY_PROJECT"),
                   dataset = Sys.getenv("BIGQUERY_DATASET"),
                   table = destination.partition)

      lapply(ga.properties, function(p) {
        sql.exec <- sprintf(sql, p, d, gaGetShop(ga.properties, p)) # Replace placeholder in sql template.
        query_exec(query = sql.exec,
                   project = Sys.getenv("BIGQUERY_PROJECT"),
                   default_dataset = Sys.getenv("BIGQUERY_DATASET"),
                   destination_table =paste0(Sys.getenv("BIGQUERY_DATASET"), ".", destination.partition),
                   max_pages = 1,
                   page_size = 1,
                   create_disposition = "CREATE_IF_NEEDED",
                   write_disposition = "WRITE_APPEND")
      })
    })

  return (res)
}

#' Inserts data into BigQuery table
#'
#' @export
#' @param table name of the target table
#' @param data data to be inserted
#' @param append specifies if data should be appended or truncated
#' @param job.name name of the ETL job that will be written to the metadata execution log
#' @param increment.field specifies field that is used for incremental data loads
#' @return results of execution
bqInsertData <- function(table, data, append = TRUE, job.name = NULL, increment.field = NULL) {

  if(xor(is.null(job.name), is.null(increment.field))) {
    stop("increment.field and job.name arguments are both required if one is provided.")
  }

  write.disposition <- ifelse(append, "WRITE_APPEND", "WRITE_TRUNCATE")
  rows <- nrow(data)
  if(rows > 0) {
    job <- insert_upload_job(project = Sys.getenv("BIGQUERY_PROJECT"),
                             dataset = Sys.getenv("BIGQUERY_DATASET"),
                             table,
                             data,
                             write_disposition = write.disposition,
                             create_disposition = "CREATE_IF_NEEDED")

    res <- wait_for(job)

    if(!is.null(job.name)) {
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
bqGetColumnNames <- function(table) {
  # Function which returns the columns of a table


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

#' Copies table
#'
#' @export
#' @param from name of the source table
#' @param to name of the desitnation table
#' @return result of the exectuion
bqCopyTable <- function(from, to) {
  # Function to copy a table in BigQuery
  # returns TRUE if the table has been succesfully copied
  src <- list(project_id = Sys.getenv("BIGQUERY_PROJECT"),
              dataset_id = Sys.getenv("BIGQUERY_DATASET"),
              table_id = from)
  dest <- list(project_id = Sys.getenv("BIGQUERY_PROJECT"),
               dataset_id = Sys.getenv("BIGQUERY_DATASET"),
               table_id = to)

  job <- copy_table( src = src,
                     dest = dest,
                     write_disposition = "WRITE_TRUNCATE",
                     project = Sys.getenv("BIGQUERY_PROJECT"))

  wait_for(job)

  return (bqTableExists(to))
}

#' Returns a case clause based on binning the input vector
#' to n+1 bins
#'
#' @export
#'
#' @param field field name to be used for the binning
#' @param limits vector of seperator values
#' @param names bin names for the given limits
#' @param res.name resulting field name for the case
#'
#' @return case clause to be included in a SQL statement
#'
bqVectorToCase <- function(field, limits, names, res.name) {
  res <- "CASE "
  mainBody <- paste0("WHEN (", field, " <= ", limits[1], ") THEN '", names[1], "' ")

  if (length(limits) > 1) {
    for (i in 2:(length(limits)) - 1) {
      tmp <- paste0("WHEN (", field, " > ", limits[i], " AND ", field, " <= ", limits[i+1], ") THEN '", names[i+1], "' ")
      mainBody <- paste0(mainBody, tmp)
    }
  }

  mainBody <- paste0(mainBody, "WHEN (", field, " > ", limits[length(limits)], ") THEN '", names[length(names)], "' ")

  res <- paste0(res, mainBody, "END AS ", res.name)
  return (res)
}
