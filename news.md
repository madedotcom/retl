# RETL Package Updates

## 0.1.4.9000

* `bqTransformPartition()`, `bqRefreshPartitionData()` - added parameter to control sql dialect of BigQuery. 
 
## 0.1.4

* `bqCreateTable()` 
  - you can switch between SQL dialects. Parameters are not available yet.
  - `write_disposition` argument was renamed to `write.disposition`.
* `bqExecuteQuery()`, `bqExecuteSql()`, `bqExecuteFile()` - (#93, @byapparov)
  - Resulting column names in data.table conformed to have words separated by dot: `my.field.name`;
  - `use.legacy.sql` argument is available to switch between SQL dialects in BigQuery.
  - named arguments to these functions will be turned to query params if standard dialect is used.
  
* `getExistingDates()` - depricated and removed in favour of `bqExistingPartitionDates()`;

* `bqGetData()` - depricated in favour of `bqExecuteQuery()` and `bqExecuteFile()`;

* `bqGetColumnNames()` - depricated, could not find it to be used. lower level calls are depricated also.

## 0.1.3

* `bqImportData()` - allows to import GS file into BigQuery table. By default imports mirror file from `table-name.csv.gz`. Format and compression params control file extension. 

* `bqExtractTable()` - allows to save table to GS file, you only need to specify table name and format, everything else will be mapped automatically.

* `getExistingPartitionDates()` - replaced by `bqExistingPartitionDates()`.

* `gaGetShop()` - is removed from the package. map of datasets should be created externaly.

* `bqInsertData()` - lost `job.name` and `increment.field` parameters as all etl logging fucntions moved to `rmeta`.

* All functions related to metadata logging and dependant on InfluxDb were moved to [rmeta](https://github.com/byapparov/rmeta) package.

* `gdLoadReport()` - function is moved to [rGoodData](https://github.com/byapparov/rGoodData) package.

* `bqRefreshPartitionData()`, `bqTransformPartition()` - priority parameter added to the functions (#86).

* `createRangeTable()` this function was fully replaced by `bqCreatePartitionTable()` and `bqTransformPartition()` functions (#86).

* `bqCreatePartitionTable()` - added priority parameter that allows to execute biquery jobs in BATCH mode.

* `dcPredictionBody()` - function is vectorised, which means that it can turn multiple transactions
  into a single body request. This is a breaking change as `custom.metrics` param is now 
  a list of vectors.

## 0.1.2

* `s3GetFile()`, `s3PutFile()` - functions are changed to read data based on the extention of the file. Fore example you can use it instead of calling `s3GetFile.csv()` if path ends with `.csv`.

* `bqInsertData()` - added fields parameter to force BigQuery types to the given types

* `disaggregate()` - new function to split data.table from aggregated to individual lines

* `bqSaveSchema()`, `bqExtractSchema()` - allow to save schema from a given data set into a JSON file

* `bqDeleteDataset()` - before deletion presense of `delete:never` label key-value pair is checked, 
    which will protect datasets from programmatic deletion. (#79)
    
* `bqProjectDatasets()` - lists datasets in the project. (#79)
    
* `bqProjectTables()` - extracts metadata for all tables in the project by extracting `__TABLES__` for each dataset. (#79)

* `bqUseLegacySql()` - allows to check or set flavour of bigquery sql. (#79)

* `s3ListFiles()` - gets metadata of s3 files matching give path into data.table (#80)

## 0.1.1

* Lower level BigQuery API calls are updated to the new functions from bigrquery 1.0.0

* `bqDeleteDataset()` deletes dataset (#71)

* `bqCreateDataset()` creates dataset (#71)

* `bqTableSchema()` loads table schema `bq_fields` object

## 0.1.0

* `influxLog()`, `influxConnection()` changed the API of the influx wrapper functions to default values. (#64, @byapparov)

* `bqRefreshPartitionData()` new function to allow batch updates of the partitioned table data. (#66, @byapparov)
