library(bigrquery)

# Required environment variables to use BigQuery helper functions:
# BIGQUERY_PROJECT - name of the project in BigQuery.
# BIGQUERY_DATASET - name of the default dataset in BigQuery.

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

getExistingDates <- function(dataset, table.prefix) {
  # Gets list of dates for which date range table exists.
  project <- Sys.getenv("BIGQUERY_PROJECT")
  tables <- list_tables(project, dataset, 10000)
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


getLastID <- function(table, field) {
  dataset <- Sys.getenv("BIGQUERY_DATASET")
  sql.tempalte <- "SELECT MAX(%1$s) as ID FROM %2$s.%3$s"
  sql <- sprintf(sql.tempalte, field, dataset, table)
  res <- query_exec(sql, project = Sys.getenv("BIGQUERY_PROJECT"))
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

bqDeleteTable <- function(table.name) {
  res <- delete_table(project = Sys.getenv("BIGQUERY_PROJECT"),
                      dataset = Sys.getenv("BIGQUERY_DATASET"),
                      table = table.name)
  return(res)
}

bqCreateTable <- function(sql, table.name) {
  # Creates table from the given SQL.
  res <- query_exec(query = sql,
                    project = Sys.getenv("BIGQUERY_PROJECT"),
                    default_dataset = Sys.getenv("BIGQUERY_DATASET"),
                    destination_table = paste0(Sys.getenv("BIGQUERY_DATASET"), ".", table.name),
                    max_pages = 1,
                    page_size = 1)
  return(res)
}


