#' @import googleAuthR
#' @import googlesheets4
NULL


#' Authentication for google sheets. Requires access_token.json path as env. var.
gsAuth <- function() {
  if (!googlesheets4::gs4_has_token()) {
    googlesheets4::gs4_auth(
      path = bqTokenFile(),
      scope = c(
        "https://www.googleapis.com/auth/drive",
        "https://spreadsheets.google.com/feeds"
      )
    )
  }
}

#' Loads google spreadsheet via key
#' @export
#' @import googlesheets4
#' @param key Something that identifies a Google Sheet: its file ID, a URL from which we can recover the ID, an instance of googlesheets4_spreadsheet (returned by gs4_get()), or a dribble, which is how googledrive represents Drive files. Processed through as_sheets_id().
#' @param tab Sheet to read, in the sense of "worksheet" or "tab". You can identify a sheet by name, with a string, or by position, with a number. Ignored if the sheet is specified via range. If neither argument specifies the sheet, defaults to the first visible sheet.
gsLoadSheet <- function(key, tab) {
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
#' @param key Something that identifies a Google Sheet: its file ID, a URL from which we can recover the ID, an instance of googlesheets4_spreadsheet (returned by gs4_get()), or a dribble, which is how googledrive represents Drive files. Processed through as_sheets_id().
gsLoadAll <- function(key) {
  gsAuth()
  tabs <- gs4_get(key)$sheets$name
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
