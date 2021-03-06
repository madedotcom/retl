% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bigquery-table.R
\name{bqTable}
\alias{bqTable}
\alias{bqTableExists}
\alias{bqDeleteTable}
\alias{bqInitiateTable}
\alias{bqPatchTable}
\alias{bqTableSchema}
\title{Functions to work with BigQuery tables}
\usage{
bqTableExists(table, dataset = bqDefaultDataset())

bqDeleteTable(table, dataset = bqDefaultDataset())

bqInitiateTable(
  table,
  schema.file,
  partition = FALSE,
  dataset = bqDefaultDataset(),
  clustering = NULL
)

bqPatchTable(table, schema.file, dataset = bqDefaultDataset())

bqTableSchema(table, dataset = bqDefaultDataset())
}
\arguments{
\item{table}{name of the table}

\item{dataset}{name of the dataset}

\item{schema.file}{path to file with the table schema}

\item{partition}{time partitioned table will be created if set to TRUE}

\item{clustering}{list with fields that will be used for clustering}
}
\value{
`bqTableExists` TRUE if table exists

`bqDeleteTable` TRUE if table was deleted
}
\description{
Family of functions for common operations on tables
}
\details{
`bqInitiateTable()` - creates table from schema file.
  If table already exists it will attempt to patch the table with new fields.
  Will fail if schema file is missing fields compared to the target table.

`bqPatchTable()`
  Adds new fields to a BigQuery table from a schema file.
  Will raise an exception if fields are removed from the schema file,
  but are still present in the target table.
}
\seealso{
https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#schema.fields
}
