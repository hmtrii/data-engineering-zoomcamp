CREATE OR REPLACE EXTERNAL TABLE `dtc-de-373401.dezoomcamp2.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dtc-de-373401/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

--Q1
SELECT COUNT(*) from `dtc-de-373401.dezoomcamp2.external_fhv_tripdata`;

--Q2
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dtc-de-373401.dezoomcamp2.external_fhv_tripdata`;

CREATE OR REPLACE TABLE `dtc-de-373401.dezoomcamp2.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `dtc-de-373401.dezoomcamp2.external_fhv_tripdata`;

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dtc-de-373401.dezoomcamp2.fhv_nonpartitioned_tripdata`;


--Q3
SELECT COUNT(*) FROM `dtc-de-373401.dezoomcamp2.external_fhv_tripdata`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL

--Q5
SELECT COUNT(DISTINCT(Affiliated_base_number)) 
FROM `dtc-de-373401.dezoomcamp2.fhv_nonpartitioned_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

--Create partition table
CREATE OR REPLACE TABLE `dtc-de-373401.dezoomcamp2.fhv_partitioned_tripdata`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS (
  SELECT * FROM `dtc-de-373401.dezoomcamp2.external_fhv_tripdata`
);

SELECT COUNT(DISTINCT(Affiliated_base_number)) 
FROM `dtc-de-373401.dezoomcamp2.fhv_partitioned_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
