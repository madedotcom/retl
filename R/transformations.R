#' Function to safely append values to a data.table from another data.table via keys
#'
#' @export
#'
#' @param data data.table to which fields will be appended
#' @param lookup data.table from which fields will be appended
#' @param by fields that represent the keys
#' @param select fields that will be appended
#' @return data set with new fields
safeLookup <- function(data,
                       lookup,
                       by,
                       select = setdiff(colnames(lookup), by)) {
  assert_that(
    is.data.table(data),
    is.data.table(lookup),
    msg = "safeLookup does not support data.frames, convert to data.table"
  )

  assert_that(
    sum(duplicated(lookup, by = by)) == 0,
    msg = "The 'by' parameter must uniquely link the two data frames"
  )

  if (nrow(data) == 0) {
    warning("Left side data frame must have non-zero number of records.")
    data <- cbind(data, lookup[, mget(select)][0])
    return(data)
  }

  lookup[, mget(c(by, select))][data, on = by]
}

#' Replace " ", "-" and "_" with a given separator in the header.
#' @export
#' @param names column names to correct.
#' @param separator char to use between words in column names.
conformHeader <- function(names, separator = ".") {
  gsub("[\\.| |_|-]", separator, tolower(names))
}

#' Splits table rows into multiple by the integer "by".
#'   All other fields are duplicated.
#'
#' @export
#' @param dt data.table to disaggregate.
#' @param by integer by which each line is split into units of 1.
#' @param keep.row.number boolean whether to keep the original
#'   row number as 'rn' in the final table
disaggregate <- function(dt, by, keep.row.number = FALSE) {
  rn <- NULL
  if (keep.row.number) dt[, rn := .I]
  res <- dt[as.integer(rep(row.names(dt), dt[, get(by)])), ]
  res[, eval(by) := 1]

  stopifnot(sum(res[, get(by)]) == sum(dt[, get(by)]))
  return(res)
}
