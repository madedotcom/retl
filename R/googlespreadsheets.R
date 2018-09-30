#' @import googleAuthR
#' @import googlesheets

gs_env <- new.env(parent = emptyenv())

#' Authentication for google sheets. Requires access_token.json path as env. var.
gsAuth <- function() {
  if (is.null(gs_env$access.cred)) {
    service_token <- gar_auth_service(
      json_file = Sys.getenv("BIGQUERY_ACCESS_TOKEN_PATH"),
      scope = c("https://www.googleapis.com/auth/drive",
                "https://spreadsheets.google.com/feeds")
      )
    gs_env$access.cred <- gs_auth(token = service_token)
  }
}

#' Loads google spreadsheet via key
#' @export
#' @import googlesheets
#' @param key key of the google spreadsheet
#' @param tab name of the tab from which data will be loaded
gsLoadSheet <- function(key, tab) {
  gsAuth()
  gap <- gs_key(key, verbose = TRUE)
  res <- gs_read(ss = gap, ws = tab)
  data.table(res)
}

#' Loads all sheets from a google spreadsheet into a list with the tab name as the list element name.
#' @export
#' @import googlesheets
#' @param key Key of the google spreadsheet.
gsLoadAll <- function(key) {
  gsAuth()
  tabs <- gs_key(key)$ws$ws_title
  sheets <- lapply(tabs, function(sheet) {
    gsLoadSheet(key = key, tab = sheet)
  })
  names(sheets) <- tabs
  sheets
}
