# Data_engineer_practice_2
My Data Engineering Zoomcamp Homework 2 Solutions (using 2022 version with airflow as orchestration tool) 

**Question 1.**
Start date for the Yellow taxi data (1 point)  
You'll need to parametrize the DAG for processing the yellow taxi data that we created in the videos.  
  
What should be the start date for this dag?

2019-01-01  
2020-01-01  
2021-01-01  
days_ago(1)


**Answer:**
- 2019-01-01
  
```
yellow_taxi_data_dag = DAG(
    dag_id = "yellow_taxi_data_v2",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2019, 1, 1),
    default_args=default_args,
    catchup = True,
    max_active_runs = 2,
    tags = ["dte-de"]
)
```

**Qustion 2.**
Frequency for the Yellow taxi data (1 point)  
How often do we need to run this DAG?

Daily  
Monthly  
Yearly  
Once

**Answer:**
- monthly
 schedule_interval = "0 6 2 * *". Translates to 6:00am, on the second day of the month, of any day of the week, and every month.



**Question 3.***
DAG for FHV Data (2 points)
Now create another DAG - for uploading the FHV data.

We will need three steps:

Download the data
Parquetize it
Upload to GCS
If you don't have a GCP account, for local ingestion you'll need two steps:

Download the data
Ingest to Postgres
Use the same frequency and the start date as for the yellow taxi dataset

**Question: how many DAG runs are green for data in 2019 after finishing everything?**
**Answer**
- 12
```
fhv_taxi_data_dag = DAG(
    dag_id = "fhv_taxi_data",
    schedule_interval = "0 7 2 * *",
    start_date = datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup = True,
    max_active_runs = 2,
    tags = ["dte-de"]
)

```

**Question 4.**
DAG for Zones (2 points)
Create the final DAG - for Zones:

Download it
Parquetize
Upload to GCS
(Or two steps for local ingestion: download -> ingest to postgres)

How often does it need to run?

Daily  
Monthly  
Yearly  
Once

**Answer**
- Once
```
zones_data_dag = DAG(
    dag_id = "zones_data",
    schedule_interval = "@once",
    start_date = days_ago(1),
    default_args=default_args,
    catchup = True,
    max_active_runs = 2,
    tags = ["dte-de"]
)
```

**Screenhsots to show all files were successfully uploaded to Google Cloud Storage Buckets!**  

![image](https://github.com/user-attachments/assets/639bb7fa-2190-4f0e-9d8b-729afe1ac650)
