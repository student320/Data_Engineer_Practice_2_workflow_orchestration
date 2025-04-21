#import libraries
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import gzip
import shutil
from datetime import datetime

#get/set enviroment variables from docker-compose file gathered from terraform files

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")




def unzip_csv_gz(src_file, dest_file):
    """
    Unzips the .csv.gz file to a .csv file.
    
    Args:
        src_file (str): The path to the  source csv.gz file after download using curl Bash command.
        dest_file (str): The path to the uncompressed output csv file.
    """
    if src_file.endswith(".gz"):
        with gzip.open(src_file, 'rb') as f_in:
            with open(dest_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
    elif src_file.endswith(".csv"):
        print("File is already a .csv, skipping unzip.")
    else:
        raise ValueError("Unsupported file format")

#note dest_file of unzip_csv_gz function will be src_file of format_to_parquet function
def format_to_parquet(src_file, dest_file):
    """
    converts csv file into parquet format.

    This function reads a csv file, converts it into a PyArrow table, 
    and then writes that table to Parquet file.

    Args:
        src_file (str): The path to the source csv file to be converted.
        dest_file (str): The path to the output parquet file.
    Raises:
        ValueError: If the input file is not csv file.
    """
    if not src_file.endswith('.csv'):
        logging.error('Can only accept csv file format for the moment')
        return
    table  = pv.read_csv(src_file)
    pq.write_table(table, dest_file)
    logging.info(f"Converted {src_file} to {dest_file} successfully.")




def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    Uploads a local file to a specified Google Cloud Storage (GCS) bucket.

    This function uploads a file from the local system to a GCS bucket. The file is
    uploaded as an object with the specified object name (which also includes the path
    within the bucket).

    The function includes a workaround to prevent timeouts for large files (>6MB) when
    uploading from systems with slow upload speeds. The workaround adjusts the chunk size
    for file uploads.

    Args:
        bucket (str): The name of the GCS bucket to which the file will be uploaded.
        object_name (str): The destination path and file name in the GCS bucket.
        local_file (str): The local file path of the file to be uploaded.

    Returns:
        None: This function does not return any value. The file is uploaded directly to GCS.
    
    Example:
        upload_to_gcs(
            bucket='my_bucket',
            object_name='raw/datafile.parquet',
            local_file='/local/path/to/datafile.parquet'
        )
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()  # Creates a connection to Google Cloud Storage (GCS) using the client object
    bucket = client.bucket(bucket)  # Retrieves an existing bucket object by name, allowing interaction with that bucket in GCS
    blob = bucket.blob(object_name)  # Creates a blob reference (empty file reference or object) inside the bucket at the specified object_name (path and filename)
    blob.upload_from_filename(local_file)  # Uploads the local file to the specified blob (file location) in the GCS bucket


#create dict for default args
default_args = {
    "owner": "airflow",
   # "start_date": datetime(2019,1,1),
    "depends_on_past": False,
    "retries": 1,
}



# Create a function that defines DAG tasks and dependencies
# NOTE: does not actually create dags yet, 
# only creates function that takes dag as parameter and defines tasks


def download_parquetize_upload_dag(
        dag,
        url_template,
        local_csv_gz_path_template,
        local_csv_path_template,
        local_parquet_path_template,
        gcs_path_template_path
):
        with dag:

            #specify tasks within DAG

            #downloads csv from url
            download_dataset_task = BashOperator(
                task_id="download_dataset_task",
                bash_command=f"curl -sSLf {url_template} > {local_csv_gz_path_template}"
            )

            gzip_task = PythonOperator(
                task_id = 'gzip',
                python_callable = unzip_csv_gz,
                op_kwargs = {'src_file':f'{local_csv_gz_path_template}',
                            'dest_file':f'{local_csv_path_template}'}
            )
        


            #uses downloaded csv as src_file and converts it to parquet
            format_to_parquet_task = PythonOperator(
                task_id="format_to_parquet_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{local_csv_path_template}",
                    "dest_file": f"{local_parquet_path_template}",
                },
            )

            # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
            #gets parquet file stored on local system and uplaods to gcs
            local_to_gcs_task = PythonOperator(
                task_id="local_to_gcs_task",
                python_callable=upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": gcs_path_template_path, #destination
                    "local_file": local_parquet_path_template, #source
                },
            )

            rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
            )


            # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            #     task_id="bigquery_external_table_task",
            #     table_resource={
            #         #destinations
            #         "tableReference": {
            #             "projectId": PROJECT_ID,
            #             "datasetId": BIGQUERY_DATASET,
            #             "tableId": "external_table",
            #         },
            #         #source after being uploaded to gcs from local
            #         "externalDataConfiguration": {
            #             "sourceFormat": "PARQUET",
            #             "sourceUris": [f"gs://{BUCKET}/{raw/{parquet_file}}"], #references data from gcs bucket
            #         },
            #     },
            # )

            download_dataset_task >> gzip_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task #>> bigquery_external_table_task



# For tellow taxi datasets--------------------------------------------------------------------------------------------------------
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + "/yellow/yellow_tripdata_{{execution_date.strftime('%Y-%m')}}.csv.gz"
YELLOW_TAXI_CSV_GZ_FILE_TEMPLATE = AIRFLOW_HOME + "/yellow_tripdata_{{execution_date.strftime('%Y-%m')}}.csv.gz"
YELLOW_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + "/yellow_tripdata_{{execution_date.strftime('%Y-%m')}}.csv"
YELLOW_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/yellow_tripdata_{{execution_date.strftime('%Y-%m')}}.parquet"
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{execution_date.strftime('%Y')}}/yellow_tripdata_{{execution_date.strftime('%Y-%m')}}.parquet"
    # with DAG(
    #     dag_id="data_ingestion_gcs_dag_v02",
    #     schedule_interval="0 6 2 * *", # =(minute, hour, day of month, every month, any week)
    #     default_args=default_args,
    #     catchup=True,
    #     max_active_runs=2,
    #     tags=['dtc-de'],
    # ) as dag:


#create DAGS
yellow_taxi_data_dag = DAG(
    dag_id = "yellow_taxi_data_v2",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2019, 1, 1),
    default_args=default_args,
    catchup = True,
    max_active_runs = 2,
    tags = ["dte-de"]
)

#call function with tasks
download_parquetize_upload_dag(
        dag= yellow_taxi_data_dag,
        url_template= YELLOW_TAXI_URL_TEMPLATE,
        local_csv_gz_path_template=YELLOW_TAXI_CSV_GZ_FILE_TEMPLATE,
        local_csv_path_template=YELLOW_TAXI_CSV_FILE_TEMPLATE,
        local_parquet_path_template=YELLOW_TAXI_PARQUET_FILE_TEMPLATE,
        gcs_path_template_path=YELLOW_TAXI_GCS_PATH_TEMPLATE
)


#For green taxi datasets----------------------------------------------------------------------------------------
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + "/green/green_tripdata_{{execution_date.strftime('%Y-%m')}}.csv.gz"
GREEN_TAXI_CSV_GZ_FILE_TEMPLATE = AIRFLOW_HOME +  "/green_tripdata_{{execution_date.strftime('%Y-%m')}}.csv.gz"
GREEN_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + "/green_tripdata_{{execution_date.strftime('%Y-%m')}}.csv"
GREEN_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/green_tripdata_{{execution_date.strftime('%Y-%m')}}.parquet"
GREEN_TAXI_GCS_PATH_TEMPLATE = "raw/green_tripdata/{{execution_date.strftime('%Y')}}/green_tripdata_{{execution_date.strftime('%Y-%m')}}.parquet"


#create DAGS
green_taxi_data_dag = DAG(
    dag_id = "green_taxi_data",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2019, 1, 1),
    default_args=default_args,
    catchup = True,
    max_active_runs = 2,
    tags = ["dte-de"]
)

#call function with tasks
download_parquetize_upload_dag(
        dag= green_taxi_data_dag,
        url_template= GREEN_TAXI_URL_TEMPLATE,
        local_csv_gz_path_template=GREEN_TAXI_CSV_GZ_FILE_TEMPLATE,
        local_csv_path_template=GREEN_TAXI_CSV_FILE_TEMPLATE,
        local_parquet_path_template=GREEN_TAXI_PARQUET_FILE_TEMPLATE,
        gcs_path_template_path=GREEN_TAXI_GCS_PATH_TEMPLATE
)


#For FHV datasets----------------------------------------------------------------------------------------
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
FHV_TAXI_URL_TEMPLATE = URL_PREFIX + "/fhv/fhv_tripdata_{{execution_date.strftime('%Y-%m')}}.csv.gz"
FHV_TAXI_CSV_GZ_FILE_TEMPLATE = AIRFLOW_HOME +  "/fhv_tripdata_{{execution_date.strftime('%Y-%m')}}.csv.gz"
FHV_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME +  "/fhv_tripdata_{{execution_date.strftime('%Y-%m')}}.csv"
FHV_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/fhv_tripdata_{{execution_date.strftime('%Y-%m')}}.parquet"
FHV_TAXI_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{execution_date.strftime('%Y')}}/fhv_tripdata_{{execution_date.strftime('%Y-%m')}}.parquet"


#create DAGS
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

#call function with tasks
download_parquetize_upload_dag(
        dag= fhv_taxi_data_dag,
        url_template= FHV_TAXI_URL_TEMPLATE,
        local_csv_gz_path_template=FHV_TAXI_CSV_GZ_FILE_TEMPLATE,
        local_csv_path_template=FHV_TAXI_CSV_FILE_TEMPLATE,
        local_parquet_path_template=FHV_TAXI_PARQUET_FILE_TEMPLATE,
        gcs_path_template_path=FHV_TAXI_GCS_PATH_TEMPLATE
)



#for zone dataset-----------------------------------------------------------------------------------------------

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc"
ZONES_TAXI_URL_TEMPLATE = URL_PREFIX + "/taxi_zone_lookup.csv"
ZONES_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME +  "/taxi_zone_lookup.csv"
ZONES_TAXI_CSV_FILE_TEMPLATE_ = AIRFLOW_HOME +   "/taxi_zone_lookup.csv"
ZONES_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME +  "/taxi_zone_lookup.parquet"
ZONES_TAXI_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.parquet"


#create DAGS
zones_data_dag = DAG(
    dag_id = "zones_data",
    schedule_interval = "@once",
    start_date = days_ago(1),
    default_args=default_args,
    catchup = True,
    max_active_runs = 2,
    tags = ["dte-de"]
)

#call function with tasks
download_parquetize_upload_dag(
        dag= zones_data_dag,
        url_template= ZONES_TAXI_URL_TEMPLATE,
        local_csv_gz_path_template=ZONES_TAXI_CSV_FILE_TEMPLATE,
        local_csv_path_template=ZONES_TAXI_CSV_FILE_TEMPLATE_,
        local_parquet_path_template=ZONES_TAXI_PARQUET_FILE_TEMPLATE,
        gcs_path_template_path=ZONES_TAXI_GCS_PATH_TEMPLATE
)