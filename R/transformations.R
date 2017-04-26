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
  df <- data.frame(data)
  lookup <- data.frame(lookup)
  if(sum(duplicated(lookup[, by])) != 0) {
    stop("The 'by' parameter must uniquely link the two data frames.")
  }

  if(nrow(df) == 0) {
    stop("Left side data frame must have non-zero number of recrods.")
  }

  tempColName <- paste0(rep(0, max(sapply(colnames(df), nchar)) +1), collapse = "")

  df[tempColName] <- 1:nrow(df)
  res <- merge(df, lookup[, c(by, select)], by = by, all.x = T)
  res <- res[order(res[tempColName][,1]), -grep(tempColName, colnames(res))]
  if(is.data.table(data)==TRUE) {res <- data.table(res)}
  return (res)
}