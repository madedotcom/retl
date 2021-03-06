% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/aws-s3.R
\name{s3ListFiles}
\alias{s3ListFiles}
\title{Lists files that match given path}
\usage{
s3ListFiles(path, bucket = s3DefaultBucket(), root = s3DefaultRoot())
}
\arguments{
\item{path}{is the path to the S3 object}

\item{bucket}{name of the S3 bucket.
Defaults to value in \code{AWS_S3_BUCKET} environment variable.}

\item{root}{project root path that is appended before the path in the argument.
Defaults to value in \code{AWS_S3_ROOT} environment variable.}
}
\value{
metadata of files in data.table
}
\description{
Lists files that match given path
}
