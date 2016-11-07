## Purpose ##

ETL project provides means to log data related job executions and facilitates incremental data processing.


## Schema ##

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
