#' @import influxdbr
#' @importFrom xts xtsAttributes

library(influxdbr)
library(xts)


retl_env <- new.env(parent = emptyenv())


#' Creates db connection based on the environmental variables
#' @param scheme look at influx_connection
#' @param host look at influx_connection
#' @param user look at influx_connection
#' @param port look at influx_connection
#' @param pwd look at influx_connection
#'
#' @export
influxConnection <- function(scheme = "http",
                             host = Sys.getenv("INFLUX_HOST"),
                             user = Sys.getenv("INFLUX_USER"),
                             port = 8086,
                             pwd = Sys.getenv("INFLUX_PASSWORD")) {
  if (!is.null(retl_env$influx_conn)) {
    return(retl_env$influx_conn)
  }
  # Creates connection to the influxdb in test consul
  retl_env$influx_conn <-
    influxdbr::influx_connection(
      scheme = scheme,
      host = host,
      port = port,
      user = user,
      pass = pwd
    )
  retl_env$influx_conn

}


#' Logs the result of a job in the influx metric table
#'@param con a connection object.
#'  defaults to connection that was set via `influxConnection()` call
#'@param db the database to write, defaults to value in `INFLUX_DB` env var
#'@param job the name of the job to log
#'@param val the variable to log
#'@param metric the metric to be written to, defaults to value in `INFLUX_METRIC` env var
#'@param env the environment to write test/prod, defaults to value in `INFLUX_PROJECT` env var
#'
#'@export
influxLog <- function(con = influxConnection(),
                      db = Sys.getenv("INFLUX_DB"),
                      job,
                      val,
                      metric = Sys.getenv("INFLUX_METRIC"),
                      env = Sys.getenv("INFLUX_PROJECT")) {
  data <- data.table(time = Sys.time(), value = val)

  xts.data <- as.xts.data.table(data)

  xts::xtsAttributes(xts.data) <- list(job = job, env = env)

  influxdbr::influx_write(
    con = con,
    db = db,
    xts = xts.data,
    measurement = metric
  )
}
