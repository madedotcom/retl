[![Build Status](https://travis-ci.org/madedotcom/retl.svg?branch=master)](https://travis-ci.org/madedotcom/retl)
[![codecov.io](https://codecov.io/github/madedotcom/retl/coverage.svg?branch=master)](https://codecov.io/github/madedotcom/retl?branch=master)

## Purpose ##

ETL project provides means to:

- exchange data with different sources through packages: aws.s3, bigrquery, DBI.
- log metadata related to job executions to facilitate incremental data processing.
- defensive data transformations that preserve original granularity or total of a metric.

## BigQuery ##

To access BigQuery you will need following environment variables:

- `BIGQUERY_PROJECT` - name of the project in BigQuery. Default project cannot be changed in the code.
- `BIGQUERY_DATASET` - name of the default dataset in BigQuery. You can override default dataset in most functions.
- `BIGQUERY_ACCESS_TOKEN_PATH` - path to the json token file.

BigQuery functions wrap `bigrquery` functions to provide higher level API removing boilerplate instructions of the lower level API.

### query data

```R
# Running query to get the sie of group A
dt <- bqExecuteQuery("SELECT COUNT(*) as size FROM my_table WHERE group = `%1$s`", "A")

# You can also save template of the query in a file and get results like this
dt <-  bqExecuteFile("group-size.sql", "A")
```

### dataset

```R
# Check if default dataset exists
bqDatasetExists()

# Create dataset
bqCreateDataset("my_dataset")

# Drop dataset
bqDeleteDataset("my_dataset")
```

### transform partitioned data

If your raw data is in daily partitioned tables you can transform
data into a new partitioned table with transformation defined in the
query template file. Date of each partition is passed into template as
first parameter in `yyyymmdd` format.

```sql
-- transformation_count.sql
SELECT COUNT(*) AS rows FROM my_table$%1$s
```

```R
# etl.R
bqTransformPartition("my_new_table_1", "transformation_count.sql")
```

### Create tables

```R
# Initiate empty table from json schema
bqInitiateTable("new_talbe", "new_table_chema.json", partition = TRUE)

# Create table from results of a query
bqCreateTable("SELECT * FROM my_table", "my_table_2")

# Create table by uploading data.table
bqInsertData("my_table", cars)
```

### Upload data

```R
bqInsertData("my_table", cars)
```

## AWS S3 ##

To access AWS S3 storage provide following environment variables:

- `AWS_ACCESS_KEY_ID` - Access key
- `AWS_SECRET_ACCESS_KEY` - Secret key
- `AWS_DEFAULT_REGION` - Default region (e.g. `us-east-1`)
- `AWS_S3_BUCKET` - Name of the S3 bucket
- `AWS_S3_ROOT` - Root path which will be added to your path. If you full path is "myproject/data/file_a.csv", you can access it as `s3GetFile("data/file_a.csv")` if root variable is set to `myproject/`.


## Schema for metadata ##

### etl_jobs ###

Field Name | Type | Description
-----------|------|------------
__job__ | STRING | Key for the job. Example: `crm.tranfer.cutomers`.
__increment_name__ | STRING | Name of the field that is used as incrment key.
__increment_type__ |STRING | Type of the field that is used as incrment key. Acceptable values: `INTEGER`, `DATE`.


### etl_increments ###

Field Name | Type | Description
-----------|------|------------
__job__ | STRING | Foreign key to the `etl_jobs` table.
__increment_value__ | STRING | maximum value in of the incrment key in the processed dataset.
__records__ |INTEGER | Number of records processed by the etl job.
__datetime__ | TIMESTAMP | Time when job was executed.

### model_performance ###

Field Name | Type | Description
-----------|------|------------
__date__ | DATE | Cross-validation execution date.
__project__ | STRING | Name of the project.
__model__ | STRING | This is usually the name of a business metric, e.g. `Revenue Forecast`
__metric__ | STRING | This is a statistical metric of the model, e.g. `MAE`.
__group__ | STRING | Name of the group within the population. If data is not split use `All`.
__size__ | INTEGER | Size of the group on which cross-validation was performed.
__value__ | FLOAT | Numeric value of the metric.
__dataset__ | STRING | Name of the dataset source if available as table in BigQuery.
