#' Read CSV file from FTP
#'
#' Reads CSV file from FTP(S) to data.table
#'
#' @export
#' @param path to the file including or excluding the storage and domain details
#' @param header defines whether file has header.
#' @return data.table
#' @name ftp-read
ftpRead <- function(path, header = TRUE) {
  fread(
    ftpFullUrl(path),
    header = header
  )
}


#' Base url to FTP based on the environment variables
#'
#' FTP(s) path is created for specified storage and environment variables
#'
#' @name ftp-url
#' @export
#'
#' @details Environment variables required for FTP(S) access:
#'  * `FTP_USER` - User name
#'  * `FTP_PASSWORD` = Password
#'  * `FTP_SERVER` = FTP domain
#'
#' @param storage - defines whether files will be read from FTP or FTPS. Defaults to `ftps`.
ftpBaseUrl <- function(storage = "ftps") {
  paste0(
    storage , "://",
    Sys.getenv("FTP_USER"),
    ":",
    Sys.getenv("FTP_PASSWORD"),
    "@",
    Sys.getenv("FTP_SERVER")
  )
}

#' Gets full URL to the FTP object
#'
#' @rdname ftp-url
#' @export
#' @param path relative or full path to the FTP object or folder
ftpFullUrl <- function(path) {
  ifelse(
    grepl("^ftp(s)?:\\/\\/", path),
    path,
    paste0(
      ftpBaseUrl(),
      "/",
      path
    )
  )
}

#' Lists files that match given regex pattern in FTP
#'
#' Recursively scans given FTP folder for files that match specified regex pattern
#' @rdname ftp-url
#'
#' @export
#' @param pattern regex pattern for file name filter
#' @param verbose defines whether FTP curl feedback will be printed
#' @param level internal parameter to track subfolder levels
ftpListFiles <- function(path, pattern, verbose = FALSE, level = 0) {
  ftp.url <- ftpFullUrl(path)
  paths <- RCurl::getURL(
    ftp.url,
    ftp.use.epsv = FALSE,
    verbose = verbose,
    ftplistonly = TRUE,
    crlf = TRUE,
    async = TRUE
  )

  if (is.null(names(paths)) && length(paths) == 1) names(paths) <- path

  subdirs <- sapply(names(paths), function(parent) {
    subpaths <- paths[[parent]]
    subpaths <- unlist(stringi::stri_split_lines(subpaths))
    pathExtractDirs(subpaths, parent)
  })
  subdirs <- unlist(subdirs, use.names = FALSE)

  files <- sapply(names(paths), function(parent) {
    subpaths <- paths[[parent]]
    subpaths <- unlist(stringi::stri_split_lines(subpaths))
    pathExtractFiles(subpaths, parent, pattern)
  })
  files <- unlist(files, use.names = FALSE)
  subfolder.files <- character()
  if (length(subdirs) > 0) {
    print(subdirs)
    subfolder.files <-
      ftpListFiles(
        subdirs,
        pattern,
        verbose,
        level = level + 1
      )
  }
  unlist(c(files, subfolder.files), recursive = TRUE)
}


pathExtractDirs <- function(paths, parent) {
  subdirs <- paths[grep("\\.", paths, invert = TRUE)]
  subdirs <- subdirs[nchar(subdirs) > 0]
  if (length(subdirs) > 0) {
    paste0(parent, subdirs, "/")
  }
  else {
    subdirs
  }
}

pathExtractFiles <- function(paths, parent, pattern) {
  files <- paths[grep("^.*\\..{1,4}$", paths)]
  files <- files[grep(pattern, paths)]
  if (length(files) > 0) {
    files <-  paste0(parent, files)
  }
  files
}
