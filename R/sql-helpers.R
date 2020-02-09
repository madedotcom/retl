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
#' @inheritParams sqlRangeLabel
#' @param alias name of the target label field
#' @return case clause to be included in a SQL statement
bqVectorToCase <- function(field, limits, alias = field) {
  paste0(
    sqlRangeTransform(field, limits, labels = caseLabel),
    " AS ",
    alias
  )
}

#' Functions that allow to convert numeric vector in to SQL case statement
#'
#' @description
#' Creates CASE statement that turns numberic field into ranges
#'
#' @param field field name to be used for the binning
#' @param limits vector of separator values
#' @param labels function that turns index, low
#' @export
#' @rdname sqlRange
sqlRangeLabel <- function(field, limits, labels = rangeLabel) {
  sqlRangeTransform(
    field,
    limits,
    labels
  )
}

#' @description
#' Creates CASE statement that turns index for the range field
#'
#' @export
#' @rdname sqlRange
sqlRangeIndex <- function(field, limits, labels = rangeIndex) {
  sqlRangeTransform(
    field = field,
    limits = limits,
    labels = labels
  )
}

#' Creates CASE statement that turns numberic field into ranges
#' @noRd
sqlRangeTransform <- function(field, limits, labels) {
  assert_that(
    length(limits) > 0,
    is.numeric(limits),
    is.character(field)
  )

  limits <- c(NA, -Inf, limits, Inf)

  case.body <- sapply(1:(length(limits) - 1), function(i) {
    value <- labels(i, limits[i], limits[i + 1])
    quote.char <- ifelse(is.integer(value), "", "'")
    paste0(
      " WHEN ",
      caseCondition(field, limits[i], limits[i + 1]),
      " THEN ",
      quote.char,
      value,
      quote.char
    )
  })

  case.body <- paste0(case.body, collapse = "")
  paste0("CASE ", case.body, " END")
}

#' Creates single condition statement
#' for given range values
#'
#' @param field name of the field in the table
#' @param low lower limit of the range (exclusive)
#' @param high highest limit of the range (inclusive)
#' @noRd
caseCondition <- function(field, low, high) {
  if (is.na(low)) {
    return(
      paste0(field, " IS NULL ")
    )
  }
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

rangeLabel <- function(index, low, high) {
  if (is.na(low)) {
    "(unknown)"
  } else {
    paste0("(", low, ", ", high, caseRightBracket(high))
  }
}

rangeIndex <- function(index, low, high) {
  index - 1
}

caseLabel <- function(index, low, high) {
  if (is.na(low)) {
    "(unknown)"
  } else {
    paste0(LETTERS[index - 1], ") (", low, ", ", high, caseRightBracket(high))
  }
}

caseRightBracket <- function(high) {
  ifelse(is.infinite(high), ")", "]")
}

getInString <- function(x) {
  paste0("'", paste(x, collapse = "', '"), "'")
}

#' Glues variables passed in ellipsis into a text of the file
#'
#' @export
#' @param file file with text to glue
#' @param ... named parameters for glue
#' @return text with values replaced based the template
#' @importFrom glue glue
readSqlGlue <- function(file, ...) {
  sql <- paste(readLines(file), collapse = "\n")
  if (length(list(...)) > 0) {
    args <- list2env(list(...))
    # template requires parameters.
    glue::glue(sql, .envir = args)
  } else {
    # template does not have parameteres.
    sql
  }
}
