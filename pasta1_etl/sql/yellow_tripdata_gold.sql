CREATE EXTERNAL TABLE IF NOT EXISTS yellow_tripdata_gold (
    VendorID TINYINT,
    Passenger_count INT,
    Total_amount DOUBLE,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    pickup_year INT,
    pickup_month INT
)
STORED BY 'io.delta.hive.DeltaStorageHandler'
PARTITIONED BY (pickup_year, pickup_month) --Adjust if you are not partitioning
LOCATION 's3://prd_yellow_taxi_table/yellow_tripdata_gold'; --Adjust bucket and table name accordingly