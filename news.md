# retl 0.1.3

* `bqExtractTable()` - allows to save table to GS file, you only need to specify table name and format, everything else will be mapped automatically.

* All functions related to metadata logging and dependant on InfluxDb were moved to [rmeta](https://github.com/byapparov/rmeta) package.

* `gdLoadReport()` - function is moved to [rGoodData](https://github.com/byapparov/rGoodData) package.

* `bqRefreshPartitionData()`, `bqTransformPartition()` - priority parameter added to the functions (#86).

* `createRangeTable()` this function was fully replaced by `bqCreatePartitionTable()` and `bqTransformPartition()` functions (#86).

* `bqCreatePartitionTable()` - added priority parameter that allows to execute biquery jobs in BATCH mode.

* `dcPredictionBody()` - function is vectorised, which means that it can turn multiple transactions
  into a single body request. This is a breaking change as `custom.metrics` param is now 
  a list of vectors.

# retl 0.1.2

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

# retl 0.1.1

* Lower level BigQuery API calls are updated to the new functions from bigrquery 1.0.0

* `bqDeleteDataset()` deletes dataset (#71)

* `bqCreateDataset()` creates dataset (#71)

* `bqTableSchema()` loads table schema `bq_fields` object

# retl 0.1.0

* `influxLog()`, `influxConnection()` changed the API of the influx wrapper functions to default values. (#64, @byapparov)

* `bqRefreshPartitionData()` new function to allow batch updates of the partitioned table data. (#66, @byapparov)
