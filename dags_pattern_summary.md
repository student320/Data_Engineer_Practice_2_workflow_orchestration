### General Pattern for Modular Airflow DAGs

### Import libraries

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
```

### Get environment variables

```BUCKET = os.environ.get("GCP_GCS_BUCKET", "default-bucket-name")```


### Create reusable functions for tasks
```
def format_to_parquet(src_file, dest_file):
    # Your logic here
    pass

def upload_to_gcs(bucket, object_name, local_file):
    # Your logic here
    pass
```

### Set default_args (optional but common)
```default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}
```

### Create a function that defines DAG tasks and dependencies
```
def build_dag(dag, url_template, local_csv, local_parquet, gcs_path):
    with dag:
        download_task = BashOperator(...)
        convert_task = PythonOperator(...)
        upload_task = PythonOperator(...)
        cleanup_task = BashOperator(...)

        download_task >> convert_task >> upload_task >> cleanup_task
```

###  Create the DAG instance(s)
```
data_ingestion_dag = DAG(
    dag_id="data_ingestion_gcs_dag_v02",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=False
)
```

###  Define your template values / arguments
```
url_template = "https://example.com/data.csv"
local_csv = "/tmp/data.csv"
local_parquet = "/tmp/data.parquet"
gcs_path = "raw/data.parquet"
```

### Call your DAG-building function
```build_dag(data_ingestion_dag, url_template, local_csv, local_parquet, gcs_path)```
🧠 Extra tip:
This structure is great for dynamic DAGs or DAG factories, where you might want to generate multiple DAGs with different parameters using a loop. Super scalable.


### REGULAR DAG pattern with context manager
1. Import Libraries
```
You import everything you need for the DAG and tasks.
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
```

2. Define default_args (Optional but common)
This is where you set default arguments for your tasks (like retries, start date, etc.).

```default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}
```
3. Create the DAG Instance
This is where you define the DAG itself and set up properties like the schedule interval and default arguments.
```
with DAG(
    "basic_dag_example",
    default_args=default_args,
    schedule_interval="@daily",  # Example schedule
    catchup=False  # Don't run past dates (useful for testing)
) as dag:

```
```
    # Task definitions go here
dag_id: Unique identifier for the DAG.

schedule_interval: How often the DAG runs.

default_args: Arguments passed to each task.

catchup=False: Prevents Airflow from running missed DAG executions.
```

4. Define Tasks Inside the with DAG Block
```
Each task is defined inside the with block. Here, you can use operators to perform your actual work.
    task_1 = BashOperator(
        task_id="task_1",
        bash_command="echo 'Task 1 is running!'"
    )

    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo 'Task 2 is running!'"
    )
```
Each task here is created and assigned to the DAG context.

5. Set Task Dependencies
You define the task dependencies using the >> or << operators, meaning task order.
```    task_1 >> task_2  # task_1 must run before task_2```
This ensures that task_1 runs first, and once it completes, task_2 will run.

6. The DAG Execution Context Ends Here
When the with DAG(...) as dag: block ends, the DAG is complete, and Airflow can start scheduling and running it.


### DAG = DAG pattern
```
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
```

### Default arguments for the DAG
```
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}
```
### Define the DAG explicitly
```
dag = DAG(
    "example_dag",
    default_args=default_args,
    schedule_interval="@daily",
)
```
### Define tasks, associating them with the DAG explicitly using `dag=dag`
```
task_1 = BashOperator(
    task_id="task_1",
    bash_command="echo 'Running task 1!'",
    dag=dag,  # Explicitly specify the DAG
)

task_2 = BashOperator(
    task_id="task_2",
    bash_command="echo 'Running task 2!'",
    dag=dag,  # Explicitly specify the DAG
)

# Set task dependencies
task_1 >> task_2
```
