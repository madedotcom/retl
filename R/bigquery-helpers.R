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

#' Counts any duplicate rows in a table when grouped by key(s)
#'
#' @export
#' @param table name of the table
#' @param dataset name of the dataset
#' @param keys vector of key columns
#' @return Total number of duplicate rows when grouped by keys
bqCountDuplicates <- function(table, dataset = bqDefaultDataset(), keys) {
  keys.string <- paste(keys, collapse=", ")
  sql <- "
  SELECT
    SUM(dup_count) AS total
  FROM (
    SELECT
      %1$s,
      (COUNT(*) - 1) AS dup_count
    FROM
      %2$s.%3$s
    GROUP BY
      %1$s
  ) s"
  duplicate_count <- bqExecuteQuery(
    sql,
    keys.string,
    dataset,
    table,
    use.legacy.sql = FALSE
  )
  res <- duplicate_count$total
  return(res)
}
