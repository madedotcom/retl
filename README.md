[![Build Status](https://travis-ci.org/madedotcom/retl.svg?branch=master)](https://travis-ci.org/madedotcom/retl)
[![codecov.io](https://codecov.io/github/madedotcom/retl/coverage.svg?branch=master)](https://codecov.io/github/madedotcom/retl?branch=master)

## Purpose ##

ETL project provides means to:

- exchange data with different sources through packages: 
    * [bigrquery](https://github.com/r-dbi/bigrquery)
    * [aws.s3](https://github.com/cloudyr/aws.s3)
- defensive data transformations that preserve original granularity or total of a metric.

## BigQuery ##

To access BigQuery you will need following environment variables:

- `BIGQUERY_PROJECT` - name of the project in BigQuery. Default project cannot be changed in the code.
- `BIGQUERY_DATASET` - name of the default dataset in BigQuery. You can override default dataset in most functions.
- `BIGQUERY_ACCESS_TOKEN_PATH` - path to the json token file.
- `BIGQUERY_LEGACY_SQL` - query will be executed with legacy flavour if set to `TRUE`.

BigQuery functions wrap `bigrquery` functions to provide higher level API removing boilerplate instructions of the lower level API.

### query data

You can parameterise your SQL using positional matching (`sprintf`) if you don't name arguments in the call to `bqExecuteQuery()`:

```R
# Running query to get the sie of group A
dt <- bqExecuteQuery("SELECT COUNT(*) as size FROM my_table WHERE group = `%1$s`", "A")

# You can also save template of the query in a file and get results like this
dt <-  bqExecuteFile("group-size.sql", "A")
```

You can use [parameters](https://cloud.google.com/bigquery/docs/parameterized-queries) in the query template with standard SQL. You have to give matching names to arguments in `bqExecuteQuery()` call:

```R
# Running query to get the sie of group A
dt <- bqExecuteQuery(
  sql = "SELECT COUNT(*) as size FROM my_table WHERE group = @group", 
  group = "A",
  use.legacy.sql = FALSE
)
```

In the example above `group` argument will be matched with `@group` paramter and BigQuery will execute 
the following query:

```SQL
SELECT 
  COUNT(*) as size 
FROM 
  my_table
WHERE 
  group = 'A' -- parameter is replaced by matching argument in the call
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

You can protect dataset from programmatic deletion by adding `delete:never` label (key:value) to it.

### Load data through storage

`bqDownloadQuery()` & `bqDownloadQueryFile()` allow downloads of large datasets through Storage.

You will need to provide environment variables to support GCS processing:

```
GCS_DEFAULT_BUCKET={gcs bucket where tables will exported to}
GCS_AUTH_FILE={path to json file with service token}
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


### Load and save data

read data from csv file into data.table
```R
# s3://{AWS_S3_BUCKET}/{AWS_S3_ROOT}/path/to/myfile.csv
dt <- s3GetFile("path/to/myfile.csv")
```

write data from data.table into a csv file
```R
# s3://{AWS_S3_BUCKET}/{AWS_S3_ROOT}/path/to/myfile.csv
dt <- s3PutFile(dt, "path/to/myfile.csv")
```

`s3GetFile` and `s3PutFile` support the following file extentions:

* **csv** - comma delimited files
* **rds** - serialised R object
* **gz** - gzip compressed csv file
* **json.gz** - compressed json file
* **zip** - zip commpressed csv file


`s3GetData` Allows to load data from many files that start with the same path

```R
# s3://{AWS_S3_BUCKET}/{AWS_S3_ROOT}/path/to/myfile_1.csv
# s3://{AWS_S3_BUCKET}/{AWS_S3_ROOT}/path/to/myfile_2.csv 
# s3://{AWS_S3_BUCKET}/{AWS_S3_ROOT}/path/to/myfile_3.csv
dt <- s3GetData("path/to/myfile_")
```

## Release process ##

### .Renviron ###

`.Renviron` file is needed and need to be configured as below:

```
RETL_PASSWORD=
BIGQUERY_ACCESS_TOKEN_PATH=
BIGQUERY_TEST_PROJECT=
BIGQUERY_METADATA_DATASET=
BIGQUERY_START_DATE=
BIGQUERY_END_DATE=

GCS_AUTH_FILE=
GCS_DEFAULT_BUCKET=

GITHUB_PAT=

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=
AWS_S3_BUCKET=
AWS_S3_ROOT=
```

### Steps to follow ###

1. Modify/Add the function in the appropriate .R file in the R/ folder. 
2. Update news.md (version, description)
3. Updated DESCRIPTION (version, data, Suggests and Imports if needed)
4. Run devtools::document().
5. Push your changes and create a PR against master branch. 
