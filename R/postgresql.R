#' @import DBI
#' @import RPostgreSQL

#' Creates db connection based on the environment variables
#' @export
dbGetConnection <- function() {
  # Creates connection to ERP db using environment variables.
  drv <- dbDriver(Sys.getenv("ERP_DB"))
  dbConnect(
    drv,
    dbname = Sys.getenv("ERP_DB_NAME"),
    host = Sys.getenv("ERP_DB_HOST"),
    port = Sys.getenv("ERP_DB_PORT"),
    user = Sys.getenv("ERP_DB_USER"),
    password = Sys.getenv("ERP_DB_PASSWORD")
  )
}

#' Executes statement in the SQL file
#'
#' @export
#' @param file path to the sql file
#' @param ... any parameters that will be used
#'     to fill in placeholders with sprintf
dbExecuteQueryFile <- function(file, ...) {
  query <- paste0(readLines(file), collapse = "\n")
  return(dbExecuteQuery(query, ...))
}

#' Executes SQL query against the database
#'
#' @export
#' @param sql string with sql query
#' @param ... any parameters that will be used to fill in placeholders with sprintf
dbExecuteQuery <- function(sql, ...) {

  if (length(list(...)) > 0) {
    # template requires parameters.
    sql <- sprintf(sql, ...)
  } else {
    # template does not have parameteres.
    sql <- sql
  }

  con <- dbGetConnection()
  on.exit(dbDisconnect(con))
  results <- dbGetQuery(con, sql)

  # replace underscore in colnames with dot.
  colnames(results) <- lapply(colnames(results), function(name) {
    gsub("_", ".", name)
  })
  data.table(results)
}
