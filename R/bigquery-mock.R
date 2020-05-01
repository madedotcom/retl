library(testthat)

#' Executes given query against stub data from a given query file
#'
#' @param x query to execute
#' @param data.query.path path to a query with stub data
#' @export
with_mock_bigquery <- function(x, data.query.path) { # nolint
  testthat::with_mock(
    `retl::bqPrepareQuery` = make_bq_dataset_query_stub(data.query.path),
    eval(x, envir = parent.frame())
  )
}


#' Creates a function that wraps a given query into a query with stub data
#'
#' @param data.query.path path to the file with stub data query
#' @noRd
make_bq_dataset_query_stub <- function(data.query.path) { # nolint
  function(query) {
    paste0(
      "WITH ",
      readSql(data.query.path),
      "(", query, ")"
    )
  }
}
