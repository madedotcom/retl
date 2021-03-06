% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bigquery-helpers.R
\name{bqAssertUnique}
\alias{bqAssertUnique}
\title{Checks for duplicates found on primary key}
\usage{
bqAssertUnique(table, dataset = bqDefaultDataset(), keys)
}
\arguments{
\item{table}{name of the table}

\item{dataset}{name of the dataset}

\item{keys}{vector of column names}
}
\value{
Throws exception if duplicates are found on primary key
}
\description{
Checks for duplicates found on primary key
}
