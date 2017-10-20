#' Function to safely append values to a data set from another data set via keys
#'
#' @export
#'
#' @param data data set to which fields will be appended
#' @param lookup data set from which fields will be appended
#' @param by fields that represent the keys
#' @param select fields taht will be appended
#' @return data set with new fields
#' TODO @Daniel : add unit tests
safeLookup <- function(data, lookup, by, select = setdiff(colnames(lookup), by)) {
  # Merges data to lookup making sure that the number of rows does not change. Keeps the original row order.

  if(sum(duplicated(lookup, by = by)) != 0) {
    stop("The 'by' parameter must uniquely link the two data frames.")
  }

  if(nrow(data) == 0) {
    stop("Left side data frame must have non-zero number of recrods.")
  }

  if(!is.data.table(data) | !is.data.table(lookup)) {
    stop("safeLookup does not support data.frames. Convert datasets to data.table")
  }

  tempColName <- paste0(rep(0, max(sapply(colnames(data), nchar)) +1), collapse = "")

  data[, tempColName :=  1:.N]
  res <- merge(data, lookup[, mget(c(by, select))], by = by, all.x = T)
  res <- setorder(res,-tempColName)
  res[, tempColName := NULL]
  return (res)
}
