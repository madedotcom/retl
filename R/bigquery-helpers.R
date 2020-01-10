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

#' Checks that there are no duplicates in a table's key(s)
#'
#'
#' @export
#' @param table name of the table
#' @param dataset name of the dataset
#' @param ... comma separated list of key columns
#' @return TRUE if the table has no duplicate rows when grouped by keys
bqCheckUniqueness <- function(table, dataset = bqDefaultDataset(), ...) {
  keys.string <- paste(list(...), collapse=", ")    # eg keys.string = "key1, key2, key3"
  dup.query <- bqExecuteQuery(
    "SELECT
      SUM(dup_count) AS dup_sum
    FROM (
      SELECT
        %1$s,
        (COUNT(*) - 1) AS dup_count
      FROM
        %2$s.%3$s
      GROUP BY
        %1$s
    ) s",
    keys.string,
    dataset,
    table,
    use.legacy.sql = FALSE
  )

  if (is.na(dup.query$dup.sum)) {
    dup.query$dup.sum = "NA"
  }

  assert_that(
    dup.query$dup.sum == 0,
    msg = sprintf("%1$s duplicate rows when %2$s.%3$s is grouped by %4$s",
                  dup.query$dup.sum, dataset, table, keys.string)
  )
}

