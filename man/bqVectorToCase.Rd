% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sql-helpers.R
\name{bqVectorToCase}
\alias{bqVectorToCase}
\title{Returns a case clause based on binning the input vector
to n+1 bins}
\usage{
bqVectorToCase(field, limits, alias = field)
}
\arguments{
\item{field}{field name to be used for the binning}

\item{limits}{vector of separator values}

\item{alias}{name of the target label field}
}
\value{
case clause to be included in a SQL statement
}
\description{
Returns a case clause based on binning the input vector
to n+1 bins
}
