#' @import googleAuthR
#' @import googlesheets
NULL

gs.env <- new.env(parent = emptyenv())

#' Authentication for google sheets. Requires access_token.json path as env. var.
gsAuth <- function() {
  if (is.null(gs.env$access.cred)) {
    service.token <- gar_auth_service(
      json_file = Sys.getenv("BIGQUERY_ACCESS_TOKEN_PATH"),
      scope = c(
        "https://www.googleapis.com/auth/drive",
        "https://spreadsheets.google.com/feeds"
      )
    )
    gs.env$access.cred <- gs_auth(token = service.token)
  }
}

#' Loads google spreadsheet via key
#' @export
#' @import googlesheets
#' @param key sheet-identifying information; a character vector of length one holding sheet title, key, browser URL or worksheets feed OR, in the case of gs_gs only, a googlesheet object
#' @param tab name of the tab from which data will be loaded
#' @param verbose logical; do you want informative messages?
#' @param lookup logical, optional. Controls whether googlesheets will place authorized API requests during registration. If unspecified, will be set to TRUE if authorization has previously been used in this R session, if working directory contains a file named .httr-oauth, or if x is a worksheets feed or googlesheet object that specifies "public" visibility.
#' @param visibility character, either "public" or "private". Consulted during explicit construction of a worksheets feed from a key, which happens only when lookup = FALSE and googlesheets is prevented from looking up information in the spreadsheets feed. If unspecified, will be set to "public" if lookup = FALSE and "private" if lookup = TRUE. Consult the API docs for more info about visibility
gsLoadSheet <- function(key,
                        tab,
                        verbose = TRUE,
                        lookup = TRUE,
                        visibility = "private") {
  gsAuth()
  gap <- gs_key(
    x = key,
    verbose = verbose,
    lookup = lookup,
    visibility = visibility
    )
  res <- gs_read(
    ss = gap,
    ws = tab
    )
  data.table(res)
}

#' Loads all sheets from a google spreadsheet into a list with the tab name as the list element name.
#' @export
#' @import googlesheets
#' @param key sheet-identifying information; a character vector of length one holding sheet title, key, browser URL or worksheets feed OR, in the case of gs_gs only, a googlesheet object
#' @param verbose logical; do you want informative messages?
#' @param lookup logical, optional. Controls whether googlesheets will place authorized API requests during registration. If unspecified, will be set to TRUE if authorization has previously been used in this R session, if working directory contains a file named .httr-oauth, or if x is a worksheets feed or googlesheet object that specifies "public" visibility.
#' @param visibility character, either "public" or "private". Consulted during explicit construction of a worksheets feed from a key, which happens only when lookup = FALSE and googlesheets is prevented from looking up information in the spreadsheets feed. If unspecified, will be set to "public" if lookup = FALSE and "private" if lookup = TRUE. Consult the API docs for more info about visibility
gsLoadAll <- function(key,
                      verbose = TRUE,
                      lookup = TRUE,
                      visibility = "private") {
  gsAuth()
  tabs <- gs_key(key)$ws$ws_title
  sheets <- lapply(tabs, function(sheet) {
    Sys.sleep(6)
    gsLoadSheet(
      key = key,
      tab = sheet,
      verbose = verbose,
      lookup = lookup,
      visibility = visibility)
  })
  names(sheets) <- tabs
  sheets
}
