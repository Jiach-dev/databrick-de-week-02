# W2_NYCTaxi_ETL_Report

## 1. Introduction
**Project Objective:**  
This project aims to perform an end-to-end ETL (Extract, Transform, Load) process on NYC Yellow Taxi trip data for January 2024. The goal is to clean, analyze, and derive insights from the dataset to support business decisions, such as demand patterns, fare analysis, and geographic trends.

**Dataset Overview:**  
The dataset includes trip records with fields like pickup/dropoff timestamps, passenger counts, trip distances, fare amounts, and location IDs. The data is sourced from the NYC TLC website in Parquet format and stored in Azure Blob Storage.

## 2. Environment & Tools
**Databricks Cluster:**  
- Runtime: `15.4 LTS (Spark 3.5.0, Scala 2.12)`  
- Node Type: `Standard_DS3_v2` (8 workers, Azure Spot instances)  
- Libraries: `Delta Lake`, `PySpark`, `Pandas`  

**Azure Storage:**  
- Account: `innovatereaildatalake`  
- Container: `nyc-taxi-data` (Geo-redundant storage, hierarchical namespace enabled)  

**Data Processing:**  
- Spark for distributed processing.  
- Delta Lake for ACID transactions and schema enforcement.  

## 3. Data Ingestion
1. **Source:** Downloaded from [NYC TLC](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet).  
2. **Upload to Azure:**  
   - Used Azure Portal to upload the Parquet file to `nyc-taxi-data/raw/yellow_taxi/2024/01`.  
3. **Loading into Spark:**  
   python
   df = spark.read.parquet("wasbs://nyc-taxi-data@innovatereaildatalake.blob.core.windows.net/raw/yellow_taxi/2024-01")
   

## 4. Data Dictionary Understanding & Initial Profiling
### Key Findings
- **Total Rows:** 2,964,624
- **Null Rates:**
  - `RatecodeID`: 4.73%
  - `passenger_count`: 4.73%
- **Outliers:** 
  - 247 negative fares
  - 62 extreme totals (>$300)

### Initial Schema
python
df.printSchema()

- `VendorID`: integer (nullable = true)
- `tpep_pickup_datetime`: timestamp_ntz (nullable = true)
- `tpep_dropoff_datetime`: timestamp_ntz (nullable = true)
- `passenger_count`: long (nullable = true)
- `trip_distance`: double (nullable = true)
- `RatecodeID`: long (nullable = true)
- `store_and_fwd_flag`: string (nullable = true)
- `PULocationID`: integer (nullable = true)
- `DOLocationID`: integer (nullable = true)
- `payment_type`: long (nullable = true)
- `fare_amount`: double (nullable = true)
- `extra`: double (nullable = true)
- `mta_tax`: double (nullable = true)
- `tip_amount`: double (nullable = true)
- `tolls_amount`: double (nullable = true)
- `improvement_surcharge`: double (nullable = true)
- `total_amount`: double (nullable = true)
- `congestion_surcharge`: double (nullable = true)
- `Airport_fee`: double (nullable = true)

## 5. Data Cleaning Strategy & Implementation
### 1. Invalid Trip Distances
**Issue:** Rows with trip distances <= 0 or >= 100.  
**Action:** Filter out these rows.  
**Justification:** Negative or zero distances are physically impossible, and extremely high values are likely errors.  
**Impact:** 2.04% of rows removed.

### 2. Missing Passenger Count
**Issue:** Rows with missing `passenger_count`.  
**Action:** Impute missing values with the median passenger count.  
**Justification:** Removing these rows would cause significant data loss.  
**Impact:** 140,162 rows imputed.

### 3. Invalid Fare Amounts
**Issue:** Rows with fare amounts <= 0.  
**Action:** Filter out these rows.  
**Justification:** Negative or zero fare amounts are not valid and likely indicate data entry errors.  
**Impact:** 1.27% of rows removed.

### 4. Missing RatecodeID
**Issue:** Rows with missing `RatecodeID`.  
**Action:** Impute missing values with the most frequent `RatecodeID`.  
**Justification:** `RatecodeID` is important for fare calculation.  
**Impact:** 140,162 rows imputed.

### 5. Outlier Passenger Counts
**Issue:** Rows with passenger counts > 6.  
**Action:** Cap passenger counts at 6.  
**Justification:** Most taxis have a maximum capacity of 6 passengers.  
**Impact:** 1.06% of rows capped.

## 6. Feature Engineering
### New Features
- **trip_duration_min:** Calculates the trip duration in minutes.  
  python
  df = df.withColumn("trip_duration_min", (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60)
  
- **avg_speed_mph:** Computes the average speed in miles per hour.  
  python
  df = df.withColumn("avg_speed_mph", col("trip_distance") / (col("trip_duration_min") / 60))
  
- **pickup_day_of_week:** Extracts the day of the week from the pickup datetime.  
  python
  df = df.withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
  
- **pickup_hour:** Extracts the hour from the pickup datetime.  
  python
  df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
  
- **time_of_day_slot:** Categorizes the pickup time into Morning, Afternoon, Evening, and Night.  
  python
  df = df.withColumn("time_of_day_slot", when(col("pickup_hour").between(5, 11), "Morning")
                                      .when(col("pickup_hour").between(12, 16), "Afternoon")
                                      .when(col("pickup_hour").between(17, 20), "Evening")
                                      .otherwise("Night"))
  

## 7. Advanced Transformations
### Scenario: Average Tip Percentage for Airport vs Non-Airport Trips
**Objective:** Calculate the average tip percentage for trips that start at airport locations versus those that don't.  
**Implementation:**  
1. Identify airport `PULocationID`s.
2. Filter trips accordingly.
3. Use window functions to get the average tip percentage for both groups.

python
airport_ids = [132, 138, 1, 2]  # Example airport location IDs
df = df.withColumn("is_airport_pickup", when(col("PULocationID").isin(airport_ids), "Airport").otherwise("Non-Airport"))
avg_tip_percentage = df.groupBy("is_airport_pickup").agg((avg(col("tip_amount") / col("fare_amount")) * 100).alias("avg_tip_percentage"))


## 8. Loading into Delta Lake
### Schema and Partitioning
**Schema:**  
- `VendorID`: integer
- `tpep_pickup_datetime`: timestamp
- `tpep_dropoff_datetime`: timestamp
- `passenger_count`: integer
- `trip_distance`: double
- `RatecodeID`: long
- `store_and_fwd_flag`: string
- `PULocationID`: integer
- `DOLocationID`: integer
- `payment_type`: string
- `fare_amount`: double
- `extra`: double
- `mta_tax`: double
- `tip_amount`: double
- `tolls_amount`: double
- `improvement_surcharge`: double
- `total_amount`: double
- `congestion_surcharge`: double
- `Airport_fee`: double
- `trip_duration_min`: decimal(27,6)
- `avg_speed_mph`: double
- `pickup_day_of_week`: integer
- `pickup_hour`: integer
- `time_of_day_slot`: string

**Partitioning Strategy:** By `pickup_year`, `pickup_month`, and `pickup_day`.  
**Justification:** Most analytical queries on taxi data are time-based. This strategy improves query performance by allowing Delta Lake to efficiently prune partitions during reads.

### DML Operations and Benefits
- **ACID Transactions:** Ensures all operations are completed successfully or none at all.
- **Time Travel:** Allows querying previous versions of the Delta Lake table.
- **OPTIMIZE/ZORDER:** Applied to improve query performance on frequently filtered columns.

## 9. Data Analysis & Insights
### Analytical Questions and Results
1. **How does average fare amount vary by time_of_day_slot and pickup_day_of_week?**
   sql
   SELECT time_of_day_slot, pickup_day_of_week, AVG(fare_amount) AS avg_fare_amount
   FROM final_taxi_df
   GROUP BY time_of_day_slot, pickup_day_of_week
   
   - **Result:** Highest average fare during Evening on Mondays.

2. **What are the top 10 busiest routes during peak hours?**
   sql
   SELECT PULocationID, DOLocationID, COUNT(*) AS trip_count
   FROM final_taxi_df
   WHERE pickup_hour BETWEEN 7 AND 9 OR pickup_hour BETWEEN 17 AND 19
   GROUP BY PULocationID, DOLocationID
   ORDER BY trip_count DESC
   LIMIT 10
   
   - **Result:** Midtown to Upper East Side is the busiest route.

3. **Is there a correlation between trip_duration_minutes and tip_amount for different payment_types?**
   sql
   SELECT payment_type, CORR(trip_duration_min, tip_amount) AS correlation
   FROM final_taxi_df
   GROUP BY payment_type
   
   - **Result:** Highest correlation for credit card payments.

4. **How do airport trips differ in terms of average distance, fare, and tip percentage compared to non-airport trips?**
   sql
   SELECT is_airport_pickup, AVG(trip_distance) AS avg_distance, AVG(fare_amount) AS avg_fare, AVG(tip_amount / fare_amount * 100) AS avg_tip_percentage
   FROM final_taxi_df
   GROUP BY is_airport_pickup
   
   - **Result:** Airport trips have higher average distance and fare but lower tip percentage.

## 10. Challenges & Learnings
### Challenges
1. **Azure Mount Conflicts:** Resolved by ensuring unique mount points.
2. **Skewed Data Distribution:** Addressed by using partitioning and bucketing.
3. **Geospatial Validation Complexity:** Utilized external libraries for accurate validation.

### Learnings
- Importance of data partitioning for performance.
- Effective handling of missing and outlier data.
- Leveraging Delta Lake features for robust data management.

## 11. Conclusion & Potential Next Steps
### Summary
We successfully performed an end-to-end ETL process on NYC Yellow Taxi trip data, addressing data quality issues, engineering new features, and loading the cleaned data into Delta Lake. Our analysis provided valuable insights into trip patterns and fare dynamics.

### Next Steps
- Integrate weather data for deeper analysis.
- Develop a real-time dashboard for monitoring taxi trip metrics.

## Appendix
### Key Code Snippets
python
# Calculate trip duration in minutes
df = df.withColumn("trip_duration_min", (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60)

# Calculate average speed in mph
df = df.withColumn("avg_speed_mph", col("trip_distance") / (col("trip_duration_min") / 60))

# Extract day of the week from pickup datetime
df = df.withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))

# Extract hour from pickup datetime
df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

# Categorize pickup time into time slots
df = df.withColumn("time_of_day_slot", when(col("pickup_hour").between(5, 11), "Morning")
                                      .when(col("pickup_hour").between(12, 16), "Afternoon")
                                      .when(col("pickup_hour").between(17, 20), "Evening")
                                      .otherwise("Night"))