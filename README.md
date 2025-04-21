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
- 
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

Question: how many DAG runs are green for data in 2019 after finishing everything?
**Answer**

**Question 4. Longest trip for each day**
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

2019-10-11
2019-10-24
2019-10-26
2019-10-31

**Answer**
```sql
SELECT
	DATE(lpep_pickup_datetime) as pickup_date,
	MAX(trip_distance) as max_distance_per_day
FROM 
	green_taxi_trips
GROUP BY 
	pickup_date
ORDER BY 
	max_distance_per_day DESC
limit 1
;
```
![image](https://github.com/user-attachments/assets/0e8394e6-25cc-4596-924f-9595fa1ecda4)



**Question 5. Three biggest pickup zones**
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
Consider only lpep_pickup_datetime when filtering by date.

**Answer**
```sql
SELECT
	tz."Zone",
	ROUND(SUM(total_amount)::numeric,2) as total_amount
FROM
	green_taxi_trips gtt
JOIN
	taxi_zone tz 
ON gtt."PULocationID" = tz."LocationID"
WHERE
	date(lpep_pickup_datetime) = '2019-10-18'
GROUP BY
	tz."Zone"
HAVING
	SUM(total_amount)::numeric > 13000
ORDER BY 
	total_amount
LIMIT 3
;
```
![image](https://github.com/user-attachments/assets/fd89f560-41dc-4770-9239-229dbb5c7d8b)

**Question 6. Largest tip**

For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

Note: it's tip , not trip

We need the name of the zone, not the ID.

Yorkville West
JFK Airport
East Harlem North
East Harlem South

**Answer**
```sql
SELECT
	tzdo."Zone",
	round(tip_amount::numeric,2) as total_tip_amount
FROM 
	green_taxi_trips gtt
JOIN
	taxi_zone tzpu
ON gtt."PULocationID" = tzpu."LocationID"
JOIN 
	taxi_zone tzdo
ON gtt."DOLocationID" = tzdo."LocationID"
WHERE
	DATE(lpep_dropoff_datetime) >='2019-10-1'
	AND DATE(lpep_dropoff_datetime) <= '2019-11-1'
	AND tzpu."Zone" = 'East Harlem North'
ORDER BY total_tip_amount DESC
limit 1
;
```
![image](https://github.com/user-attachments/assets/90ef9b50-971b-434c-910e-930d3061d2c0)

**Question 7. Terraform Workflow**
Which of the following sequences, respectively, describes the workflow for:

Downloading the provider plugins and setting up backend,
Generating proposed changes and auto-executing the plan
Remove all resources managed by terraform`

terraform import, terraform apply -y, terraform destroy
teraform init, terraform plan -auto-apply, terraform rm
terraform init, terraform run -auto-approve, terraform destroy
terraform init, terraform apply -auto-approve, terraform destroy
terraform import, terraform apply -y, terraform rm

**Answer**
terraform init, terraform apply -auto-approve, terraform destroy


