# retl 0.1.1.9000

* `s3GetFile()`, `s3PutFile()` - functions are changed to read data based on the extention of the file. Fore example you can use it instead of calling `s3GetFile.csv()` if path ends with `.csv`.

* `bqInsertData()` - added fields parameter to force BigQuery types to the given types
* `disaggregate()` - new function to split data.table from aggregated to individual lines

# retl 0.1.1

* Lower level BigQuery API calls are updated to the new functions from bigrquery 1.0.0

* `bqDeleteDataset()` deletes dataset (#71)

* `bqCreateDataset()` creates dataset (#71)

* `bqTableSchema()` loads table schema `bq_fields` object

# retl 0.1.0

* `influxLog()`, `influxConnection()` changed the API of the influx wrapper functions to default values. (#64, @byapparov)

* `bqRefreshPartitionData()` new function to allow batch updates of the partitioned table data. (#66, @byapparov)
