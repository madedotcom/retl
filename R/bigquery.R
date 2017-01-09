library(bigrquery)

# Required environment variables to use BigQuery helper functions:
# BIGQUERY_PROJECT - name of the project in BigQuery.
# BIGQUERY_DATASET - name of the default dataset in BigQuery.

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

createPartitionTable <- function(table, sql = NULL, file = NULL, existing.dates = NULL, missing.dates = NULL) {
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

  ga.properties <- getGoogleAnalyticsProperties()

  res <-
    lapply(missing.dates, function(d) { # Create partition for every missing date.
      destination.partition <- paste0(table, "$", d)
      print(paste0("Partition name: ", destination.partition))

      delete_table(project = Sys.getenv("BIGQUERY_PROJECT"),
                   dataset = Sys.getenv("BIGQUERY_DATASET"),
                   table = destination.partition)

      lapply(ga.properties, function(p) {
        sql.exec <- sprintf(sql, p, d) # Replace placeholder in sql template.
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


createRangeTable <- function(table, sql = NULL, file = NULL) {
  # Creates range teable in BigQuery.
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

  # StartDate
  start.date <- as.Date("2016-01-08")
  #EndDate
  end.date <- Sys.Date() - 1


  project <- Sys.getenv("BIGQUERY_PROJECT")
  dataset <- Sys.getenv("BIGQUERY_DATASET")

  existing.dates <- getExistingPartitionDates(dataset, table)
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

library(stringr)

getExistingDates <- function(bq.dataset, table.prefix, bq.project = Sys.getenv("BIGQUERY_PROJECT")) {
  # Gets list of dates for which date range table exists.
  tables <- list_tables(bq.project, bq.dataset, 10000)
  matches <- tables[grepl(table.prefix, tables)]
  res <- str_extract(matches,"\\d{8}")
  return(res)
}

getMissingDates <- function(start.date, end.date, existing.dates) {
  # Gets list of dates for which date range table is missing.
  days <- rep(1, end.date - start.date + 1)
  days.sequence <- seq_along(days)
  dates <- start.date -1 + days.sequence
  dates <- as.character(dates, "%Y%m%d")
  res <- setdiff(dates, existing.dates)
  return (res)
}


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

bqTableExists <- function(table.name) {
  res <- exists_table(Sys.getenv("BIGQUERY_PROJECT"),
                      dataset = Sys.getenv("BIGQUERY_DATASET"),
                      table = table.name)
  return(res)
}

bqDeleteTable <- function(bq.table, bq.project = Sys.getenv("BIGQUERY_PROJECT"),
                                      bq.dataset = Sys.getenv("BIGQUERY_DATASET")) {
  res <- delete_table(project = bq.project, dataset = bq.dataset, table = bq.table)
  return(res)
}

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


bqGetData <- function(sql = NULL, file = NULL, ...) {
  # Wrapper function to load data from BigQuery.

  if(!missing(file)) { # Gets sql from file.
    return(bqExecuteFile(file, ...))
  }
  else {
    return(dbExecuteQuery(sql, ...))
  }
}

bqExecuteFile <- function(file, ...) {
  # Function to load data from BigQuery using file with SQL.

  sql <-  paste(readLines(file), collapse="\n")
  res <- bqExecuteSql(sql, ...)
  return(res)
}

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
  return(res)
}

gaGetShop <- function(ga.properties, property) {
  # Gets the shop code from the GA properties vector.
  #
  # Parameters:
  #   ga.properties - named vector of Google Analytics properties.
  #                   Names are ISO2 codes of the country.
  #   property - is a property in Google Analytics.

  shops <- names(ga.properties)
  names(shops) <- ga.properties
  return(shops[as.character(property)])
}


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

bqInsertData <- function(table, data, append = TRUE) {
  # Function inserts data.frame into BigQuery table.

  write.disposition <- ifelse(append, "WRITE_APPEND", "WRITE_TRUNCATE")

  job <- insert_upload_job(project = Sys.getenv("BIGQUERY_PROJECT"),
                           dataset = Sys.getenv("BIGQUERY_DATASET"),
                           table,
                           data,
                           write_disposition = write.disposition,
                           create_disposition = "CREATE_IF_NEEDED")

  res <- wait_for(job)
  return(res)
}

bqGetColumnNames <- function(table) {
  # Function which returns the columns of a table


  info <- get_table(project = Sys.getenv("BIGQUERY_PROJECT"),
                    dataset = Sys.getenv("BIGQUERY_DATASET"),
                    table)

  # Unlist all the schema data and keep only the name fields
  # remove the naming and then return the vector with only
  # the names
  fields <- unlist(info$schema$fields)
  fields <- fields[names(fields)=="name"]
  names(fields) <- NULL

  return(fields)
}

