#' Gets dates that are missing from the date range
#'   for a give list of existing dates
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

#' Checks that there are no duplicates in the "id" column, the table's primary key
#'
#' @export
#' @param table name of the table
#' @param dataset name of the dataset
#' @return TRUE if the table's primary key has no duplicates
bqTablePkIsUnique <- function(table, dataset = bqDefaultDataset()) {
  duplicate.order.lines <- bqExecuteQuery(
    "SELECT
       id,
       COUNT(*) AS numrows
     FROM
       %1$s.%2$s
     GROUP BY
       id
     HAVING numrows > 1",
    dataset,
    table,
    use.legacy.sql = FALSE
  )
  
  assert_that(
    nrow(duplicate.order.lines) == 0,
    msg = paste0("Uniqueness check failed, duplicate id's found in ", dataset, ".", table)
  )
}
