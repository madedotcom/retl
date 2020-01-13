# RETL Package Updates

## 0.1.28

* `bqAssertUnique()` - New function, throws exception if duplicates are found on primary key
* `bqCountDuplicates()` - New function, returns count of duplicate rows when grouped by key(s).

## 0.1.27

* `gsLoadSheet()` - add verbose, lookup and visibility params which were taking default values and was causing issues when loading private sheet.
* `gsLoadAll()` - add verbose, lookup and visibility params to be passed to `gsLoadSheet()`.

## 0.1.26

* `s3GetFile.zip()` - add fread.fill param to function to prevent R session error on large files

## 0.1.25

* `dcListConversions()` - use env variables DOUBLECLICK_SEGMENTATION_TYPE and DOUBLECLICK_SEGMENTATION_NAME to allow to push metrics to new Doubleclick activities

## 0.1.24

* `bqInsertData()` - add initiation capability via argument schema.file
* `bqInsertLargeData()` - add initiation capability via argument schema.file

## 0.1.23

* `bqCreateTable()` - add initiation capability via argument schema.file

## 0.1.22

* `bqTransformPartition()` - made missing.dates parameter of the function

## 0.1.21

* `bqDownloadQuery()` - allows to load data from BigQuery via Storage.

## 0.1.20

* `readSqlGlue()` - new function to pass variables passed in ellipsis into a text of the file

## 0.1.19

* `bqCreatePartitionTable()` - change missing.dates parameter logic 

## 0.1.18

* `bqExecuteDml()` - takes parameters

## 0.1.17

* `bqExecuteSql()` - #127 is resolved, now you can pass parameters to query with explicit `bq_param_array` class (@byapparov, #129)

## 0.1.16

* `dcWriteCustomMetics` - add the correction for the binary read

## 0.1.15

* `sqlRangeLabel()`, `sqlRangeIndex()` - function allow to create `CASE` statements from limits vector that defines ranges (@byapparov, #122) 

## 0.1.14

* Switched to versions of bigrquery above [1.2.0](https://github.com/r-dbi/bigrquery/blob/master/NEWS.md#bigrquery-120).

* Fixed test `bqExecuteDml()`

* Added gargle for access token encryption which allows to do full bigquery test
  on Travis, see [how to manage tokens securely article](https://gargle.r-lib.org/articles/articles/managing-tokens-securely.html).

## 0.1.13

* `bqExecuteDml()` - added support for DML statement execution that can be run with 
  different priority without loading data to the server.

## 0.1.12

* `bqPatchTable()` - now uses field name and type for matching.

## 0.1.11

* `bqExecuteQuery()` and `bqExecuteFile()` - fixed to allow parameterised queries with vector values in params.

## 0.1.10

* `bqInitiateTable()` - Will fail if schema file is missing fields compared to the target table.
* `bqPatchTable()` - Function that allows to update table fields using the schema file.

## 0.1.9

* `bqCreatePartitionTable()` - added `use.legacy.sql` parameter to simplify control for sql type.

## 0.1.8

*   `bqInsertLargeData()` - new function to split large data into 'chunks' which is then inserted into the Big Query table iteratively.

## 0.1.7

*   `s3GetData()` renamed `s3Get.FUN` to `s3.get.fun` to comply with coding style.

*   Style changes and `.lintr` added. Lint checks added to the testthat to make 
  code validation part of CI.

## 0.1.5

*   `bqTransformPartition()`, `bqRefreshPartitionData()` - added parameter to control sql dialect of BigQuery. 
*   `bqCreatePartitionTable()` - updated to create partition from several shard tables 
  with one combined query.   
  This is done to reduce the number of changes against the target table to meet the 
  limit of 5000 changes per day.
 
## 0.1.4

*   `bqCreateTable()` 
  - you can switch between SQL dialects. Parameters are not available yet.
  - `write_disposition` argument was renamed to `write.disposition`.
*   `bqExecuteQuery()`, `bqExecuteSql()`, `bqExecuteFile()` - (#93, @byapparov)
  - Resulting column names in data.table conformed to have words separated by dot: `my.field.name`;
  - `use.legacy.sql` argument is available to switch between SQL dialects in BigQuery.
  - named arguments to these functions will be turned to query params if standard dialect is used.
  
*   `getExistingDates()` - depricated and removed in favour of `bqExistingPartitionDates()`;

*   `bqGetData()` - depricated in favour of `bqExecuteQuery()` and `bqExecuteFile()`;

*   `bqGetColumnNames()` - depricated, could not find it to be used. lower level calls are depricated also.

## 0.1.3

*   `bqImportData()` - allows to import GS file into BigQuery table. By default imports mirror file from `table-name.csv.gz`. Format and compression params control file extension. 

*   `bqExtractTable()` - allows to save table to GS file, you only need to specify table name and format, everything else will be mapped automatically.

*   `getExistingPartitionDates()` - replaced by `bqExistingPartitionDates()`.

*   `gaGetShop()` - is removed from the package. map of datasets should be created externaly.

*   `bqInsertData()` - lost `job.name` and `increment.field` parameters as all etl logging fucntions moved to `rmeta`.

* All functions related to metadata logging and dependant on InfluxDb were moved to [rmeta](https://github.com/byapparov/rmeta) package.

*   `gdLoadReport()` - function is moved to [rGoodData](https://github.com/byapparov/rGoodData) package.

*   `bqRefreshPartitionData()`, `bqTransformPartition()` - priority parameter added to the functions (#86).

*   `createRangeTable()` this function was fully replaced by `bqCreatePartitionTable()` and `bqTransformPartition()` functions (#86).

*   `bqCreatePartitionTable()` - added priority parameter that allows to execute biquery jobs in BATCH mode.

*   `dcPredictionBody()` - function is vectorised, which means that it can turn multiple transactions
  into a single body request. This is a breaking change as `custom.metrics` param is now 
  a list of vectors.

## 0.1.2

*   `s3GetFile()`, `s3PutFile()` - functions are changed to read data based on the extention of the file. Fore example you can use it instead of calling `s3GetFile.csv()` if path ends with `.csv`.

*   `bqInsertData()` - added fields parameter to force BigQuery types to the given types

*   `disaggregate()` - new function to split data.table from aggregated to individual lines

*   `bqSaveSchema()`, `bqExtractSchema()` - allow to save schema from a given data set into a JSON file

*   `bqDeleteDataset()` - before deletion presense of `delete:never` label key-value pair is checked, 
    which will protect datasets from programmatic deletion. (#79)
    
*   `bqProjectDatasets()` - lists datasets in the project. (#79)
    
*   `bqProjectTables()` - extracts metadata for all tables in the project by extracting `__TABLES__` for each dataset. (#79)

*   `bqUseLegacySql()` - allows to check or set flavour of bigquery sql. (#79)

*   `s3ListFiles()` - gets metadata of s3 files matching give path into data.table (#80)

## 0.1.1

*   Lower level BigQuery API calls are updated to the new functions from bigrquery 1.0.0

*   `bqDeleteDataset()` deletes dataset (#71)

*   `bqCreateDataset()` creates dataset (#71)

*   `bqTableSchema()` loads table schema `bq_fields` object

## 0.1.0

*   `influxLog()`, `influxConnection()` changed the API of the influx wrapper 
  functions to default values. (#64, @byapparov)

* `bqRefreshPartitionData()` new function to allow batch updates of the partitioned table data. (#66, @byapparov)
