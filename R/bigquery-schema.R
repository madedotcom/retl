#' Extract schema as JSON from data.table
#'
#' @rdname bqSchema
#' @export
bqExtractSchema <- function(dt) {
  if (is.data.frame(dt)) {
    dt.copy <- copy(head(dt))
    colnames(dt.copy) <- conformHeader(colnames(dt.copy), "_")
    fields <- as_bq_fields(dt.copy)
  }
  else if (is.string(dt)) {
    fields <- bqTableSchema(dt)
  }
  else if (class(dt) == "bq_fields") {
    fields <- dt
  }
  jsonlite::toJSON(bqJsonFields(fields), pretty = TRUE)
}

#' Save file inferred from data.table to JSON file
#'
#' @export
#' @rdname bqSchema
#' @param dt data.table, table name or bq_fields from which schema will be extracted into JSON
#' @param file path to the schema file
bqSaveSchema <- function(dt, file) {
  dt.schema <- bqExtractSchema(dt)
  write(dt.schema, file)
}

#' Finds asymmetric difference between two sets of bq_fields objects
#'
#' @param x bq_fields object
#' @param y bq_fields object
#' @seealso setdiff bqMatchField
#' @export
bqSetdiffSchemas <- function(x, y) {
  res <- lapply(x, function(field) {
    bqMatchField(field, y)
  })
  res <- unlist(res)
  x[!res]
}

#' Returns true of field is found in a set of fields
#'
#' Field is matched to a given set based on name and type.
#' All other attributes of fields are ignored, and this implementation
#' is not recursive which might cause problems for RECORD fields.
#'
#' @param x field to be matched
#' @param fields fields to be matched against
#' @noRd
bqMatchField <- function(x, fields) {
  for (field in fields) {
    if (x$name == field$name & x$type == field$type) {
      return(TRUE)
    }
  }
  FALSE
}

#' @noRd
bqReadSchema <- function(schema.file) {
  bqAuth()
  as_bq_fields(read_json(schema.file))
}

bqJsonField <- function(x) {
  res <- list(
    name = jsonlite::unbox(x$name),
    type = jsonlite::unbox(x$type),
    mode = jsonlite::unbox(x$mode)
  )
  if (!is.null(x$fields) & length(x$fields) > 0) {
    res$fields <- bqJsonFields(x$fields)
  }
  res
}

bqJsonFields <- function(x) {
  lapply(x, bqJsonField)
}
