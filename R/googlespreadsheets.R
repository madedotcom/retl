#' @import googleAuthR
#' @import googlesheets

library(googleAuthR)
library(googlesheets)
library(data.table)

#' Requires access_token.json file the working directory

#' Loads google spreadsheet via key
#' @export
#' @import googlesheets
#' @param key key of the google spreadsheet
#' @param tab name of the tab from which data will be loaded
#' @param token.file json access token file for Google API.
gsLoadSheet <- function(key, tab, token.file = "access_token.json") {
  service_token <- gar_auth_service(json_file = token.file, scope = c("https://www.googleapis.com/auth/drive",
                                                                      "https://spreadsheets.google.com/feeds"))
  gs_auth(token = service_token)
  gap <- gs_key(key, verbose = T)
  res <- gs_read(ss = gap, ws = tab)
  res <- data.table(res)
}

#' Loads all sheets from a google spreadsheet into a list with the tab name as the list element name.
#' @export
#' @import googlesheets
#' @param key Key of the google spreadsheet.
#' @param token.file Json access token file for Google API.
gsLoadAll <- function(key, token.file = "access_token.json") {
  tabs <- gs_key(key)$ws$ws_title
  sheets <- lapply(tabs,
                   function(sheet) {
                     dt <- gsLoadSheet(key = key,
                                       tab = sheet,
                                       token.file = token.file)
                     return(dt)})
  names(sheets) <- tabs
  return(sheets)
}
