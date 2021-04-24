from datetime import datetime, timedelta
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import bigquery
from google.oauth2 import service_account

project_id = models.Variable.get("project_id") 
bucket_folder = models.Variable.get("bucket_folder") 

def pulldf():
    credentials = service_account.Credentials.from_service_account_file(f'{bucket_folder}/pkl-playing-fields-7314d23dc2d0.json')
    client = bigquery.Client(credentials= credentials,project=project_id)
    sql = "SELECT * FROM pkl-playing-fields.unified_events.event WHERE event_name = 'purchase_item'"
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()

def pushdf():
    credentials = service_account.Credentials.from_service_account_file(f'{bucket_folder}/etl-on-cloud-c3200b4d384a.json')
    df.to_gbq(destination_table="week2.event",
                project_id = project_id,
                if_exists = "replace",
                credentials = credentials)
    
default_args = {
    "start_date": datetime(2021, 3, 21),
    "depends_on_past": True,
}

with models.DAG(
    "task-1-transform-transactional-event",
    default_args = default_args,
    schedule_interval = timedelta(days=3),
) as dag:

    pulldata = PythonOperator(
        task_id = "pull-data-from-bq",
        python_callable = pulldf
    )

    pushdata = PythonOperator(
        task_id = "push-data-to-bq",
        python_callable = pushdf
    )

    pulldata >> pushdata