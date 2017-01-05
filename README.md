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

### model_performance ###

Field Name | Type | Description
-----------|------|------------
__date__ | date | Cross-validation execution date.
__project__ | STRING | Name of the project.
__model__ | STRING | This is usually the name of a business metric, e.g. `Revenue Forecast`
__metric__ | STRING | This is a statistical metric of the model, e.g. `MAE`.
__group__ | STRING | Name of the group within the population. If data is not split use `All`.
__size__ | INTEGER | Size of the group on which cross-validation was performed.
__value__ | FLOAT | Numeric value of the metric.
