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
  retl_env$influx_conn <- influxdbr::influx_connection(scheme = scheme,
                                      host = host,
                                      port = port,
                                      user = user,
                                      pass = pwd)
  retl_env$influx_conn

}


#' Logs the result of a job in the influx metric table
#'@param con a connection object
#'@param db the database to write
#'@param job the name of the job to log
#'@param val the variable to log
#'@param metric the metric to be written to
#'@param env the environment to write test/prod
#'
#'@export
etlLogInflux <- function(con, db, job, val, metric, env){
  data <- data.table(time = Sys.time(),
                     value = val)

  xts.data <- as.xts.data.table(data)

  xts::xtsAttributes(xts.data) <- list(job = job,
                                  env = env)

  influxdbr::influx_write(con = con,
                          db = db,
                          xts = xts.data,
                          measurement = metric)
}
