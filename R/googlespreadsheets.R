#' @import googleAuthR
#' @import googlesheets4
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
#' @import googlesheets4
#' @param key Something that identifies a Google Sheet: its file ID, a URL from which we can recover the ID, an instance of googlesheets4_spreadsheet (returned by gs4_get()), or a dribble, which is how googledrive represents Drive files. Processed through as_sheets_id().
#' @param tab Sheet to read, in the sense of "worksheet" or "tab". You can identify a sheet by name, with a string, or by position, with a number. Ignored if the sheet is specified via range. If neither argument specifies the sheet, defaults to the first visible sheet.
#' @param verbose OBSOLETE
#' @param lookup OBSOLETE
#' @param visibility OBSOLETE
gsLoadSheet <- function(key,
                        tab,
                        verbose = TRUE,
                        lookup = TRUE,
                        visibility = "private") {
  gsAuth()
  res <- read_sheet(
    ss = key,
    sheet = tab
    )
  data.table(res)
}

#' Loads all sheets from a google spreadsheet into a list with the tab name as the list element name.
#' @export
#' @import googlesheets4
#' @param key sheet-identifying information; a character vector of length one holding sheet title, key, browser URL or worksheets feed OR, in the case of gs_gs only, a googlesheet object
#' @param verbose OBSOLETE
#' @param lookup OBSOLETE
#' @param visibility OBSOLETE
gsLoadAll <- function(key,
                      verbose = TRUE,
                      lookup = TRUE,
                      visibility = "private") {
  gsAuth()
  tabs <- sheets_get(key)$sheets
  sheets <- lapply(tabs, function(sheet) {
    Sys.sleep(6)
    gsLoadSheet(
      key = key,
      tab = sheet
    )
  })
  names(sheets) <- tabs
  sheets
}
