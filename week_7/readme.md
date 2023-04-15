## Dataset:
ANZ Road Crash Dataset
https://www.kaggle.com/datasets/mgray39/australia-new-zealand-road-crash-dataset

## Problem Statement
Be able to create an end-to-end data pipeline that involves the following
1. Create an ETL pipeline that gets data from a source and into a datalake (GCS)
2. Create an ETL pipeline that retrieves data from the datalake, perform cleaning and normalization of data and into a data warehouse (BigQuery)
3. Perform transformations on raw data to be able to provide the needed data to dashboards
4. Create a dashboard that consumes the transformed views of the raw data

## Data Being Explored
1. Total Crashes per vehicle type (Done)
2. Total Crashes per vehicle type per state (Done)
3. Total Crashes per state (Done)
4. Total Casualties, fatalities, major/minor injuries per vehicle type
5. Total Casualties, fatalities, major/minor injuries per state
6. Total crashes that involves the following conditions
    * road conditions
    * drug/alcohol related
    * environment conditions
    * locations

## Method
* Batch Processing using Prefect

## Requirements
1. GCP account, with project and service account with Storage Admin, Storage Object Admin and BigQuery Admin permissions with access key file downloaded
2. Prefect with GCP Block
    * `poetry add prefect-gcp`
    * `poetry run prefect block register prefect_gcp.credentials`
    * add access service account access key to the block

## Todo:

### Infrastructure:

1. Prefect and GCP storage integration for loading data into the lakehouse
2. Kafka and Faust for data streaming into datawarehouse
3. Spark for data warehousing
4. Which data analytics tool to use? (Google looker?)

### Application:
1. ETL Code from data source to GCS
2. ETL code from GCS to Data warehouse
3. DBT code to transform/create views in data warehouse
4. A Dashboard to do analytics

### Accompanying Data Dictionary
* More information on the columns can be found here: https://github.com/mgray39/anz-crash/blob/master/Australian%20%26%20New%20Zealand%20Road%20Crash.ipynb
