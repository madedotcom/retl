#' @import DBI
#' @import RPostgreSQL

library(RPostgreSQL)

#' Creates db connection based on the environment variables
#' @export
dbGetConnection <- function() {
  # Creates connection to ERP db using environment variables.
  drv <- dbDriver(Sys.getenv("ERP_DB"))
  con <- dbConnect(drv, dbname = Sys.getenv("ERP_DB_NAME"),
                   host = Sys.getenv("ERP_DB_HOST"),
                   port = Sys.getenv("ERP_DB_PORT"),
                   user = Sys.getenv("ERP_DB_USER"),
                   password = Sys.getenv("ERP_DB_PASSWORD"))
  return(con)
}

#' Executes statement in the SQL file
#'
#' @export
#' @param sql.file path to the sql file
dbExecuteQueryFile <- function(sql.file) {
  # Executes SQL query from the given file and returns the result as a data.frame.
  #
  # Args:
  #   sql.file: file name of the query.
  query <- paste0(readLines(sql.file), collapse = "\n")
  return(dbExecuteQuery(query))
}

#' Executes SQL query agains the database
#'
#' @export
#' @param query string with sql query
dbExecuteQuery <- function(query) {

  con <- dbGetConnection()
  on.exit(dbDisconnect(con))
  results <- dbGetQuery(con, query)

  # replace underscore in colnames with dot.
  colnames(results) <- lapply(colnames(results), function(name) {return (gsub("_", ".", name))})
  return(results)
}