#' @import googleAuthR
#' @import googlesheets

library(googleAuthR)
library(googlesheets)
library(data.table)

#' Authentication for google sheets. Requires access_token.json path as env. var.
gsAuth <- function() {
  service_token <- gar_auth_service(json_file = Sys.getenv("BIGQUERY_ACCESS_TOKEN_PATH"),
                                    scope = c("https://www.googleapis.com/auth/drive",
                                              "https://spreadsheets.google.com/feeds"))
  gs_auth(token = service_token)
}

#' Loads google spreadsheet via key
#' @export
#' @import googlesheets
#' @param key key of the google spreadsheet
#' @param tab name of the tab from which data will be loaded
#' @param token.file json access token file for Google API.
gsLoadSheet <- function(key, tab) {
  gsAuth()
  gap <- gs_key(key, verbose = TRUE)
  res <- gs_read(ss = gap, ws = tab)
  res <- data.table(res)
}

#' Loads all sheets from a google spreadsheet into a list with the tab name as the list element name.
#' @export
#' @import googlesheets
#' @param key Key of the google spreadsheet.
#' @param token.file Json access token file for Google API.
gsLoadAll <- function(key) {
  gsAuth()
  tabs <- gs_key(key)$ws$ws_title
  sheets <- lapply(tabs,
                   function(sheet) {
                     dt <- gsLoadSheet(key = key,
                                       tab = sheet)
                     return(dt)})
  names(sheets) <- tabs
  return(sheets)
}
