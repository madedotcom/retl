[![Build Status](https://travis-ci.org/madedotcom/retl.svg?branch=master)](https://travis-ci.org/madedotcom/retl)
[![codecov.io](https://codecov.io/github/madedotcom/retl/coverage.svg?branch=master)](https://codecov.io/github/madedotcom/retl?branch=master)

## Purpose

ETL project provides means to:

- exchange data with different sources through packages: 
    * [bigrquery](https://github.com/r-dbi/bigrquery)
    * [aws.s3](https://github.com/cloudyr/aws.s3)
- defensive data transformations that preserve original granularity or total of a metric
    * `safeLookup()` function for example is an alternative to `merge()` which enforces safe left join.

## BigQuery

To access BigQuery you will need following environment variables:

```apacheconf
#.Renviron
BIGQUERY_PROJECT # name of the project in BigQuery. Default project cannot be changed in the code.
BIGQUERY_DATASET # name of the default dataset in BigQuery. You can override default dataset in most functions.
BIGQUERY_ACCESS_TOKEN_PATH # path to the json token file.
BIGQUERY_LEGACY_SQL # query will be executed with legacy flavour if set to `TRUE`.
```

BigQuery functions wrap `bigrquery` functions to provide higher level API removing boilerplate instructions of the lower level API.

### Query data

You can parameterise your SQL using positional matching (`sprintf`) if you don't name arguments in the call to `bqExecuteQuery()`:

```R
# Running query to get the size of group A in Legacy dialect
dt <- bqExecuteQuery("SELECT COUNT(*) as size FROM my_table WHERE group = `%1$s`", "A")

# You can also save template of the query in a file and get results like this
dt <-  bqExecuteFile("group-size.sql", "A")
```

You can use [parameters](https://cloud.google.com/bigquery/docs/parameterized-queries) in the query template with standard SQL. You have to give matching names to arguments in `bqExecuteQuery()` call:

```R
# Running query to get the size of group A
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

You can also pass array into query like this:

```R
# Running query to get the size of groups A and B
dt <- bqExecuteQuery(
  sql = "SELECT COUNT(*) as size FROM my_table WHERE group IN UNNEST(@group)", 
  group = c("A", "B"),
  use.legacy.sql = FALSE
)
```

Sometimes you don't know in advance the length of the vector that will be passed into a query.
`UNNEST` in the query above is expecting array which is not created for scalars. 

To avoid an error in dynamic scripts (shiny), you can enforce array with `bq_param_array()` like this:

```R
# Running query to get the size of groups A and B
dt <- bqExecuteQuery(
  sql = "SELECT COUNT(*) as size FROM my_table WHERE group IN UNNEST(@group)", 
  group = bq_param_array(input$groups),
  use.legacy.sql = FALSE
)
```

### Working with datasets

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

```apacheconf
# .Renviron
GCS_AUTH_FILE= # path to json file with service token
GCS_BUCKET= # gcs bucket where tables will exported to
```

### Transform partitioned data

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

Range of dates that will be backfilled is limited between these envvars:

```apacheconf
# .Renviron
BIGQUERY_START_DATE=
BIGQUERY_END_DATE=
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

You can also use `bqInsertLargeData()` function.

This will load data into BigQuery table through the temp file in Google Cloud Storage:

```R
# Note: you need to load googleCloudStorageR in your pipeline
#       for authentication.
library(googleCloudStorageR)
bqInsertLargeData("my_table", cars)
``` 

Two additional environment variable are required for connection with Cloud Storage:

```apacheconf
# .Renviron
GCS_AUTH_FILE= # path to json token file to access Cloud Storage
GCS_BUCKET= # bucket where temporary json file with data will be created
```

## AWS S3

To access AWS S3 storage provide the following environment variables:

```apacheconf
# .Renviron
AWS_ACCESS_KEY_ID # Access key
AWS_SECRET_ACCESS_KEY # Secret key
AWS_DEFAULT_REGION # Default region (e.g. `us-east-1`)
AWS_S3_BUCKET # Name of the S3 bucket
AWS_S3_ROOT # Root path, see examples bellow.
```

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

## Contributing

### .Renviron

`.Renviron` file is needed should be configured to set up envars.

Here is a small example with two variables:

```apacheconf
GCS_AUTH_FILE=access_token.json
GCS_BUCKET=my-bucket
```

Full details on environment in R seessions can be found here: `help(Startup)`

### Testing

Package is setup for automated tests on Travis.

To test locally, you will need to create access tocken file through `gargle`

```apacheconf
#.Renviron
BIGQUERY_TEST_PROJECT=
RETL_PASSWORD=
```

### Release Steps

1. Modify/Add the function in the appropriate .R file in the R/ folder. 
2. Update news.md (version, description)
3. Updated DESCRIPTION (version, data, Suggests and Imports if needed)
4. Run devtools::document().
5. Push your changes and create a PR against master branch. 
