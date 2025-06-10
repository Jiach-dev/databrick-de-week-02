# Week 2 - NYC Taxi ETL Project

## Project Overview
This project performs an end-to-end ETL process on NYC Yellow Taxi trip data for January 2024. It involves data ingestion, cleaning, feature engineering, advanced transformations, and loading into Delta Lake for analysis. The goal is to extract insights on taxi trip patterns, fare dynamics, and geographic trends.

## Prerequisites
- **Azure Account:** Access to Azure Blob Storage with permissions to upload and manage data.
- **Databricks Workspace:** A Databricks cluster configured with Runtime 15.4 LTS (Spark 3.5.0, Scala 2.12) or compatible.
- **Libraries:** Delta Lake, PySpark, Pandas installed on the cluster.
- **Dataset:** Download the full dataset from [NYC TLC January 2024 Yellow Taxi Data](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet).

## Setup Instructions

### Upload Data to Azure Storage
1. Log in to your Azure Portal.
2. Navigate to your storage account (e.g., `innovatereaildatalake`).
3. Upload the Parquet file to the container `nyc-taxi-data` under the path:  
   `raw/yellow_taxi/2024/01/yellow_tripdata_2024-01.parquet`

### Configure Databricks Cluster
- Use a cluster with at least 8 workers, Standard_DS3_v2 node type recommended.
- Ensure the cluster has the required libraries installed.
- Set up access to Azure Blob Storage using appropriate credentials or mount points.

### Running the Notebook
- Update the data path in the notebook to:  
  `wasbs://nyc-taxi-data@innovatereaildatalake.blob.core.windows.net/raw/yellow_taxi/2024/01/yellow_tripdata_2024-01.parquet`
- Execute the notebook cells sequentially to perform ETL and analysis.

## Additional Resources
- Detailed project report: [W2_NYCTaxi_ETL_Report.md](./W2_NYCTaxi_ETL_Report.md)