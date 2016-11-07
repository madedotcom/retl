library(RPostgreSQL)
library(devtools)
library(aws.s3)

# Load environment variables.
dw.db          <- Sys.getenv("DW_DB")
dw.dbname      <- Sys.getenv("DW_DB_NAME")
dw.host        <- Sys.getenv("DW_HOST")
dw.port        <- Sys.getenv("DW_PORT")
s3.bucket      <- Sys.getenv("AWS_BUCKET")
s3.root.folder <- Sys.getenv("AWS_ROOT_FOLDER")

# Gets connections to the dw database.
dwGetConnection <- function() {
  dw.user     <- Sys.getenv("DW_USER")
  dw.password <- Sys.getenv("DW_PASSWORD")
  drv <- dbDriver(dw.db)
  con <- dbConnect(drv, dbname = dw.dbname, 
                   host = dw.host, 
                   port = dw.port, 
                   user = dw.user,
                   password = dw.password)
  return (con)
}

# Truncates table in the data warehouse.
dwTruncateTable <- function(table) {
  query <- paste("TRUNCATE TABLE", table, sep = " ")
  con <- dwGetConnection()
  res <- dbSendQuery(con, query)
  dbDisconnect(con)
}

dwSaveDataToTable = function (df, path, table) {
  # Copies df to S3 then saves it in the table in Redshift. File name is auto-generated.
  #
  # Args:
  #  df: dataframe to copy.
  #  path: s3 subfolder.
  #  table: table to copy the df content to.
  
  # Save data to temp file.
  file.name <- paste(paste(Sys.Date(), table, sep = "-"), ".csv", sep = "")
  
  # Put file to Amazon S3.
  # s3.folder is set in config.R file.
  s3PutFile(df, file.name, path)
  
  dwTruncateTable(table)
  
  # Copy data from S3 to customers table in Redshift.
  query <- paste0(readLines("dw/copy-data.sql"), collapse = "\n")
  
  s3.access_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  s3.secret_key <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  s3.folder <- paste0("s3://", s3.bucket, s3.root.folder, s3.path)
  query <- sprintf(query, table, s3.folder, file.name, s3.access_key, s3.secret_key)
  # dw connection parameters are set in config.R file.
  con <- dwGetConnection()
  res <- dbSendQuery(con, query)
  dbDisconnect(con)
}

s3PutFile <- function (df, file.name=df, path="", bucket=s3.bucket, root=s3.root.folder) {
  # Saves the data frame in S3.
  #   file.name: name of the file to be uploaded in S3.
  #   path: S3 path starting after root folder, excludes filename. example: "folder/".
  write.csv(df, file = file.name, row.names = F)
  full.path <- paste0(root, path, file.name)
  put_object(file = file.name, object = full.path, bucket=bucket)
  if (file.exists(file.name)) file.remove(file.name)
}

s3GetFile <- function(path, header=T, bucket=s3.bucket, root=s3.root.folder) {
  # Args:
  #   path: S3 path starting after root folder, includes filename. example: "folder/file.ext".
  full.path <- paste0(root, path)
  raw_data <- get_object(full.path, bucket)
  # In case of error, print error message.
  if(!is.raw(raw_data)) 
    stop(print(raw_data[1:3]))
  
  data <- iconv(readBin(raw_data, character()), from="UTF-8", to="UTF-8")
  df <- as.data.frame(fread(data, header=header, strip.white = F))
  # Replace " ", "-" and "_" with "." in the header.
  names(df) <- gsub(" |_|-", ".", names(df))
  return(df)
}

dwSendSelectQuery <- function(query) {
  # Function executes SQL statement against data warehouse.
  con <- dwGetConnection()
  on.exit(dbDisconnect(con))
  res <- dbSendQuery(con, query)
  val <- dbFetch(res)
  dbClearResult(res)
  return (val)
}

dwSendInsertQuery <- function(query) {
  # Function executes SQL statement against data warehouse.
  con <- dwGetConnection()
  on.exit(dbDisconnect(con))
  res <- dbSendQuery(con, query)
  return (NULL)
}

