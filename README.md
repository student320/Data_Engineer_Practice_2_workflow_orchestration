# Data_engineer_practice_2
My Data Engineering Zoomcamp Homework 2 Solutions (using 2022 version with airflow as orchestration tool) 

**Question 1.**

**Answer:**
GitBash commands
```bash
$ docker run -it python:3.12.8 bash
root@7c1f376a6587:/# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
root@7c1f376a6587:/#
```

- 24.3.1


**Qustion 2.**

Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?
```bash
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

**Answer:**
- db:5432

**Prepare Postgres:**


```bash
#"Use Docker Compose file from above to set up the environment and run postgres."
docker compose up -d 

#Ingest green taxi data and create table named green_taxi_trips in postrgesql using ingest_data_1.py python script (check python file for script).

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

python ingest_data_1.py \
    --user=postgres \
    --password=postgres \
    --host=localhost \
    --port=5433 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}

#Ingest taxi zone data and create table named taxi_zone is postgresql using ingest_data_1.py python script.

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

python ingest_data_1.py \
    --user=postgres \
    --password=postgres \
    --host=localhost \
    --port=5433 \
    --db=ny_taxi \
    --table_name=taxi_zone \
    --url=${URL}
```
**Question 3. Trip Segmentation Count**
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:
Up to 1 mile
In between 1 (exclusive) and 3 miles (inclusive),
In between 3 (exclusive) and 7 miles (inclusive),
In between 7 (exclusive) and 10 miles (inclusive),
Over 10 miles

**Answer**

```sql
SELECT
	SUM(CASE WHEN trip_distance <=1 THEN 1 ELSE 0 END) as num_up_to_1,
	SUM(CASE WHEN trip_distance >1 and trip_distance <=3 THEN 1 ELSE 0 END) as num_trips_between_1_3,
	SUM(CASE WHEN trip_distance >3 and trip_distance <=7 THEN 1 ELSE 0 END) as num_trips_between_3_7, 
	SUM(CASE WHEN trip_distance >7 and trip_distance <=10 THEN 1 ELSE 0 END) as num_trips_between_7_10, 
	SUM(CASE WHEN trip_distance >10 THEN 1 ELSE 0 END) as num_trips_over_10
FROM
	green_taxi_trips
WHERE 
    lpep_pickup_datetime >= '2019-10-01'
    and lpep_pickup_datetime < '2019-11-01'
    and lpep_dropoff_datetime >= '2019-10-01'
    and lpep_dropoff_datetime < '2019-11-01'
;
```
![image](https://github.com/user-attachments/assets/0f943858-eaaa-4ab9-a851-bb1589a65f51)


**Answer #2**
```sql
SELECT
CASE
	WHEN trip_distance <=1 THEN 'up to 1 mile'
	WHEN trip_distance >1 AND trip_distance <= 3 THEN 'between 1 and 3 miles'
	WHEN trip_distance >3 AND trip_distance <= 7 THEN 'between 3 and 7 miles'	
	WHEN trip_distance >7 AND trip_distance <= 10 THEN 'between 7 and 10 miles'	
	WHEN trip_distance >10 THEN 'over 10 miles'
END as segment,
to_char(COUNT(*), '999,999') AS num_trips
FROM
	green_taxi_trips
WHERE 
    lpep_pickup_datetime >= '2019-10-01'
    and lpep_pickup_datetime < '2019-11-01'
    and lpep_dropoff_datetime >= '2019-10-01'
    and lpep_dropoff_datetime < '2019-11-01'
GROUP BY segment
;
```
![image](https://github.com/user-attachments/assets/7eb8182a-b6fd-42ca-8670-1953f9b07320)


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


