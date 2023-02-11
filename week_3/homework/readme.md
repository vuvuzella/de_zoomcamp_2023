1. What is the count for fhv vehicle records for year 2019?

- Answer: 43244696

2.Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- Answer: 0 MB for the External Table and 317.94MB for the BQ Table

- SQL Query:

```
SELECT Affiliated_base_number, count(Affiliated_base_number) FROM `de-zoomcamp-376020.de_zoomcamp_dataset.fhv_trip_data_2019` group by Affiliated_base_number;


SELECT Affiliated_base_number, count(Affiliated_base_number) FROM `de-zoomcamp-376020.de_zoomcamp_dataset.fhv_data_2019_external` group by Affiliated_base_number;
```

3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

- Answer: 717748

- SQL Query:

```
SELECT count(1) FROM `de-zoomcamp-376020.de_zoomcamp_dataset.fhv_trip_data_2019`
WHERE PUlocationID is NULL and DOlocationID is NULL;

```

4. What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

- Answer: Partition by pickup_datetime Cluster on affiliated_base_number

5. Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

- Answer:
    - Without optimization: 647.87 MB
    - With PARTITION on pickup_datetime and CLUSTERING on Affiliated_Affiliated_base_number: 23.05 MB

```

-- Create partition table
CREATE TABLE `de-zoomcamp-376020.de_zoomcamp_dataset.PARTITIONED_fhv_trip_data_2019`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number
AS SELECT * FROM `de-zoomcamp-376020.de_zoomcamp_dataset.fhv_trip_data_2019`;

-- select on NON partitioned and non clustered table
SELECT DISTINCT Affiliated_base_number,  FROM `de_zoomcamp_dataset.fhv_trip_data_2019`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- WITH partitioning on pickup_datetime and clustering on Affiliated_base_number
SELECT DISTINCT Affiliated_base_number,  FROM `de_zoomcamp_dataset.PARTITIONED_fhv_trip_data_2019`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

```

6. Where is the data stored in the External Table you created?

- Answer: GCP Bucket

7. It is best practice in Big Query to always cluster your data:

- Answer: True
