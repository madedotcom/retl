#' Creates partition table for a given sql
#'
#' @description
#' Parameters that will be passed to SQL for the placeholders:
#'
#' %1$s - the name of the BigQuery dataset
#' %2$s - the date (YYYYMMDD) of the partition
#'
#' @export
#' @param table name of the destination table
#' @param datasets list of Google Analytics properties to populate table for
#' @param sql sql to use a source of the data
#' @param file if sql is not provided it will be read from the file
#' @param existing.dates dates that should be skipped
#' @param missing.dates dates calculation for which will be enforced
#' @param priority sets priority of job execution to INTERACTIVE or BATCH
#' @inheritParams bqExecuteSql
bqCreatePartitionTable <- function(table,
                                   datasets,
                                   sql = NULL,
                                   file = NULL,
                                   existing.dates = NULL,
                                   missing.dates = NULL,
                                   priority = "INTERACTIVE",
                                   use.legacy.sql = bqUseLegacySql()) {
  assert_that(
    xor(is.null(sql), is.null(file)),
    msg = "Either sql or file must be provided"
  )

  bqAuth()

  if (missing(sql)) {
    # Build SQL from code in the file.
    sql <- readLines(file)
  }

  if (missing(existing.dates)) {
    existing.dates <- bqExistingPartitionDates(table)
  }

  if (missing(missing.dates) || is.null(missing.dates)) {
    missing.dates <- getMissingDates(
      bqStartDate(),
      bqEndDate(),
      existing.dates
    )
  }

  jobs <-
    lapply(missing.dates, function(d) {
      # Create partition for every missing date.
      destination.partition <- bqPartitionName(table, d)
      message("Updating partition: ", destination.partition)


      sql.list <- lapply(datasets, function(p) {
        # Replace placeholders in sql template.
        sql.exec <- sprintf(sql, p, d, bqDatasetLabel(datasets, p))
        paste(sql.exec, collapse = "\n")
      })

      bqCreateTable(
        sql = bqCombineQueries(sql.list, use.legacy.sql),
        table = destination.partition,
        priority = priority,
        write.disposition = "WRITE_TRUNCATE",
        use.legacy.sql = use.legacy.sql
      )
    })

  bqWait(jobs, priority)
}

#' Gets existing dates for date partitioned table in BigQuery
#'
#' @export
#' @param table name of a table
#' @return string vector of dates
bqExistingPartitionDates <- function(table) {
  if (!bqTableExists(table)) {
    return(character())
  }
  sql <- bqPartitionDatesSql(table)
  res <- bqExecuteSql(sql)
  if (nrow(res) > 0) {
    return(res$partition.id)
  }
  else {
    return(character())
  }
}

#' Generates sql for extraction of existing partition date
#' @noRd
bqPartitionDatesSql <- function(table) {
  if (bqUseLegacySql()) {
    paste0("SELECT partition_id from [", table, "$__PARTITIONS_SUMMARY__];")
  } else {
    paste0("SELECT
              FORMAT_DATE('%Y%m%d', DATE(_PARTITIONTIME)) as partition_id
            FROM `", table, "`
            GROUP BY 1;")
  }
}

#' Functions to update existing partitions in the target table
#'
#' @description `bqRefreshPartitionData` updates existing partitions in the target table
#'
#' @rdname bqRefreshPartitionData
#' @export
#' @param table destination partition table where results of the query will be saved
#' @param file path to the sql file that will be used for the transformation
#' @param ...  parameters that will be passed via `sprintf` to build dynamic SQL.
#'    partition date will be always passed first in format `yyyymmdd`
#'    followed by arguments in `...`
#' @inheritParams bqCreateTable
bqRefreshPartitionData <- function(table,
                                   file,
                                   ...,
                                   priority = "BATCH",
                                   use.legacy.sql = bqUseLegacySql()) {
  existing.dates <- bqExistingPartitionDates(table)

  jobs <- lapply(existing.dates, function(d) {
    partition <- gsub("-", "", d)
    destination.partition <- paste0(table, "$", partition)
    sql <- readSql(file, d, ...)

    bqCreateTable(
      sql = sql,
      table = destination.partition,
      write.disposition = "WRITE_TRUNCATE",
      priority = priority,
      use.legacy.sql = use.legacy.sql
    )
  })
  bqWait(jobs, priority)
}

#' Functions to transforms partitioned data form one table to another
#'
#' @description `bqTransformPartition` creates new partitions for the missing dates
#' @rdname bqTransformPartition
#' @export
#' @param table destination partition table where results of the query will be saved
#' @param file path to the sql file that will be used for the transformation
#' @param ...  parameters that will be passed via `sprintf` to build dynamic SQL.
#'    partition date will be always passed first in format `yyyymmdd`
#'    followed by arguments in `...`
#' @param missing.dates dates for which to run this function for
#' @param priority Default to INTERACTIVE
#' @param use.legacy.sql Defaults to env variable if specified
bqTransformPartition <- function(table,
                                 file,
                                 ...,
                                 missing.dates = NULL,
                                 priority = "INTERACTIVE",
                                 use.legacy.sql = bqUseLegacySql()) {

  existing.dates <- bqExistingPartitionDates(table)
  start.date <- bqStartDate(unset = "2017-01-01")
  end.date <- bqEndDate()

  if (missing(missing.dates) || is.null(missing.dates)) {
    missing.dates <- getMissingDates(
      bqStartDate(),
      bqEndDate(),
      existing.dates,
      "%Y-%m-%d"
    )
  }

  jobs <- lapply(missing.dates, function(d) {
    partition <- gsub("-", "", d)
    destination.partition <- paste0(table, "$", partition)
    message("Updating partition: ", destination.partition)
    sql.exec <- readSql(file, partition, ...)

    bqCreateTable(
      sql.exec,
      table = destination.partition,
      write.disposition = "WRITE_TRUNCATE",
      priority = priority,
      use.legacy.sql = use.legacy.sql
    )
  })

  bqWait(jobs, priority)
}


#' Inserts data table into a specific partition of a partition table.
#'
#' @export
#' @param table Name of the table
#' @param date Partition date
#' @param data Data table to insert
#' @param append Append to the partition if TRUE else overwrite
bqInsertPartition <- function(table, date, data, append = FALSE) {
  target.partition <- bqPartitionName(table, date)

  bqInsertData(
    table = target.partition,
    data = data,
    append = append
  )
}

#' Creates partition name by combining table and partition date.
#'
#' @export
#' @param table Name of the table
#' @param date Partition date
#' @return Full partition table name
bqPartitionName <- function(table, date) {
  partition.time <- gsub("-", "", date)
  res <- paste0(table, "$", partition.time)
  return(res)
}

#' Deletes a specific partition of a partition table.
#'
#' @export
#' @param table Name of the table
#' @param date Partition date
bqDeletePartition <- function(table, date) {
  name <- bqPartitionName(table, date)
  bqDeleteTable(name)
}

#' Combines list of queries into a single query
#'
#' @noRd
#' @param sql list of queries that will be combined into one
#' @param use.legacy.sql defines which flavour to use
bqCombineQueries <- function(sql, use.legacy.sql = bqUseLegacySql()) {
  if (use.legacy.sql) {
    sql.list <- lapply(sql, function(x) {
      paste0("(", x, ")")
    })
    paste0("SELECT * FROM\n", paste0(sql.list, collapse = ",\n"))
  } else {
    paste0(sql, collapse = "\n UNION ALL \n")
  }
}

bqDatasetLabel <- function(datasets, dataset) {
  labels <- names(datasets)
  names(labels) <- datasets
  return(labels[as.character(dataset)])
}
