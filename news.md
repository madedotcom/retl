# retl 0.1.1

* Lower level BigQuery API calls are updated to the new functions from bigrquery 1.0.0

* `bqDeleteDataset()` Deletes dataset (#71).

* `bqCreateDataset()` Creates dataset (#71).

# retl 0.1.0

* `influxLog()`, `influxConnection()` changed the API of the influx wrapper functions to default values. (#64, @byapparov)

* `bqRefreshPartitionData()` new function to allow batch updates of the partitioned table data. (#66, @byapparov)
