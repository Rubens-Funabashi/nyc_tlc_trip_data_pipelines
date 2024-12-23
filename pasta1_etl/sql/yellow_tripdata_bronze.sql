CREATE EXTERNAL TABLE IF NOT EXISTS yellow_tripdata_bronze (
    VendorID TINYINT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    Passenger_count INT,
    Trip_distance DOUBLE,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    RateCodeID DOUBLE,
    Store_and_fwd_flag STRING,
    Payment_type TINYINT,
    Fare_amount DOUBLE,
    Extra DOUBLE,
    MTA_tax DOUBLE,
    Improvement_surcharge DOUBLE,
    Tip_amount DOUBLE,
    Tolls_amount DOUBLE,
    Total_amount DOUBLE,
    congestion_Surcharge DOUBLE,
    Airport_fee DOUBLE
)
STORED BY 'io.delta.hive.DeltaStorageHandler'
PARTITIONED BY (pickup_year, pickup_month) --Adjust if you are not partitioning
LOCATION 's3://prd_yellow_taxi_table/yellow_tripdata_bronze'; --Adjust bucket and table name accordingly