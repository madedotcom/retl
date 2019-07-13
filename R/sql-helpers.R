#' Creates SQL statement from the source file
#'
#' @export
#' @param file file with sql statement template
#' @param ... any parameters that will be used
#'   to fill in placeholders in the template with sprintf
#' @return SQL statement as a string
readSql <- function(file, ...) {
  sql <- paste(readLines(file), collapse = "\n")

  if (length(list(...)) > 0) {
    # template requires parameters.
    sql <- sprintf(sql, ...)
  } else {
    # template does not have parameteres.
    sql <- sql
  }

  return(sql)
}

#' Returns a case clause based on binning the input vector
#' to n+1 bins
#'
#' @export
#' @param field field name to be used for the binning
#' @param limits vector of separator values
#' @param alias resulting field name for the case
#' @return case clause to be included in a SQL statement
bqVectorToCase <- function(field, limits, alias = field) {
  assert_that(
    length(limits) > 0,
    is.numeric(limits),
    is.character(field)
  )

  limits <- c(-Inf, limits, Inf)

  case.body <- sapply(1:(length(limits) - 1), function(i) {
    paste0(
      "WHEN (",
      caseCondition(field, limits[i], limits[i + 1]),
      ") THEN '",
      caseLabel(i, limits[i], limits[i + 1])
    )
  })
  case.body <- paste0(case.body, collapse = "")

  paste0("CASE ", case.body, "END AS ", alias)
}

caseCondition <- function(field, low, high) {
  low.limit <- paste0(field, " > ", low)
  high.limit <- paste0(field, " <= ", high)
  if (is.infinite(low)) {
    return(high.limit)
  }
  if (is.infinite(high)) {
    return(low.limit)
  }
  paste0(
    low.limit, " AND ", high.limit
  )
}

caseLabel <- function(index, low, high) {
  paste0(LETTERS[index], ") (", low, ", ", high, caseRightBracket(high), "' ")
}

caseRightBracket <- function(high) {
  ifelse(is.infinite(high), ")", "]")
}

getInString <- function(x) {
  paste0("'", paste(x, collapse = "', '"), "'")
}
