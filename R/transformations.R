#' Function to safely append values to a data.table from another data.table via keys
#'
#' @export
#'
#' @param data data.table to which fields will be appended
#' @param lookup data.table from which fields will be appended
#' @param by fields that represent the keys
#' @param select fields taht will be appended
#' @return data set with new fields
safeLookup <- function(data, lookup, by, select = setdiff(colnames(lookup), by)) {

  assert_that(
    is.data.table(data),
    is.data.table(lookup)
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

  tempColName <- paste0(rep(0, max(sapply(colnames(data), nchar)) + 1), collapse = "")

  data[, eval(tempColName) :=  1:.N]
  res <- merge(data, lookup[, mget(c(by, select))], by = by, all.x = T)
  res <- res[order(get(tempColName))]
  data[, eval(tempColName) := NULL]
  res[, eval(tempColName) := NULL]
}

#' Replace " ", "-" and "_" with a given separator in the header.
#' @export
#' @param names column names to correct.
#' @param separator char to use between words in colnumn names.
conformHeader <- function(names, separator = ".") {
 gsub("[\\.| |_|-]", separator, tolower(names))
}
