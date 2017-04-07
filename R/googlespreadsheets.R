library(googleAuthR)
library(googlesheets)
library(data.table)

#' Requires access_token.json file the working directory

#' Loads google spreadsheet via key
#'
#' @param key key of the google spreadsheet
gsLoadSheet <- function(key, tab) {
  service_token <- gar_auth_service(json_file="access_token.json", scope = c("https://www.googleapis.com/auth/drive",
                                                                             "https://spreadsheets.google.com/feeds"))
  gs_auth(token = service_token)
  gap <- gs_key(key, verbose = T)
  res <- gs_read(ss = gap, ws = tab)
  res <- data.table(res)
}
