% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/aws-s3.R
\name{s3GetFile}
\alias{s3GetFile}
\alias{s3GetFile.csv}
\alias{s3GetFile.gz}
\alias{s3GetFile.rds}
\alias{s3GetFile.zip}
\alias{s3GetFile.json.gz}
\title{Loads data from AWS S3 into data.table object}
\usage{
s3GetFile(path, bucket = s3DefaultBucket(), root = s3DefaultRoot(), ...)

s3GetFile.csv(
  path,
  bucket = s3DefaultBucket(),
  root = s3DefaultRoot(),
  header = TRUE
)

s3GetFile.gz(path, bucket = s3DefaultBucket(), root = s3DefaultRoot())

s3GetFile.rds(path, bucket = s3DefaultBucket(), root = s3DefaultRoot())

s3GetFile.zip(
  path,
  bucket = s3DefaultBucket(),
  root = s3DefaultRoot(),
  fread.fill = FALSE
)

s3GetFile.json.gz(path, bucket = s3DefaultBucket(), root = s3DefaultRoot())
}
\arguments{
\item{path}{is the path to the S3 object}

\item{bucket}{name of the S3 bucket.
Defaults to value in \code{AWS_S3_BUCKET} environment variable.}

\item{root}{project root path that is appended before the path in the argument.
Defaults to value in \code{AWS_S3_ROOT} environment variable.}

\item{...}{additional arguments that will be passed to extension specific calls.}

\item{header}{flag defines whether file has header}

\item{fread.fill}{boolean value to indicate if fill param should be used. Defaults to FALSE.}
}
\value{
\code{s3GetFile} gets data from source \code{.csv} file

`s3GetFile.csv` loads data from `.csv` files

`s3GetFile.gz` loads data from `.gz` files

`s3GetFile.rds` loads data from `.rds` files

`s3GetFile.zip` loads data from `.zip` files

`s3GetFile.json.gz` loads data from `.json.gz` files
}
\description{
Convention based wrapper functions that allow to load
data files in S3 into data.table
}
\details{
Environment variables required for aws.s3 access:
\itemize{
\item \code{AWS_ACCESS_KEY_ID} = "mykey"
\item \code{AWS_SECRET_ACCESS_KEY} = "mysecretkey"
\item \code{AWS_DEFAULT_REGION} = "us-east-1"
\item \code{AWS_SESSION_TOKEN} = "mytoken"
}

\code{s3GetFile} calls extension specific files based on the path of the file.
}
\seealso{
aws.s3 package documentation for access details:
\url{https://github.com/cloudyr/aws.s3}
}
