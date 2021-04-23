import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator

bucket_input = models.Variable.get("bucket_input")
bucket_folder = models.Variable.get("bucket_folder")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")


default_args = {
    "start_date": datetime(2021, 3, 10),
    "end_date"  : datetime(2021, 3, 15), 
    "dataflow_default_options": {
        "project": project_id, 
        "region": gce_region, 
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_folder + "/tmp/",
        "numWorkers": 1,
    },
}

    
def get_date(**kwargs):
    return kwargs.get('ds-nodash')

def get_date_1(**kwargs):
    return kwargs.get('ds')

# Define a DAG (directed acyclic graph) of tasks.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_dataflow_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval='@daily',  # Override to match your needs
) as dag:


    task1 = PythonOperator(
        task_id = "get_date",
        python_callable = get_date,
        provide_context = True
    )
    
    storage_to_bigquery = DataflowTemplateOperator(
        # The task id of your job
        task_id="storage_to_bigquery",
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        # Use the link above to specify the correct parameters for your template.
        parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath": bucket_folder + "/jsonSchema.json",
            "javascriptTextTransformGcsPath": bucket_folder + "/transform.js",
            "inputFilePattern": bucket_input + "/search_"+{{execution_date }}+".csv",
            "outputTable": project_id + ":week2.search-keyword",
            "bigQueryLoadingTemporaryDirectory": bucket_folder + "/tmp/",
        },
    )
    
    task2 = PythonOperator(
        task_id = "get_date_1",
        python_callable = get_date_1#,
        provide_context = True
    )

    querying_daily_top_search = BigQueryOperator(
        task_id = "querying_daily_top_search",
        sql =  "SELECT lower(search_keyword) as keyword, count(lower(search_keyword)) as search_count, created_at"\
               "FROM `etl-on-cloud.week2.search_keyword`"\
               f"WHERE created_at = {get_date_1()}"\
               "group by keyword, created_at"\
               "order by search_count desc"\
               "limit 1;",
        use_legacy_sql = False,
        destination_dataset_table = 'etl-on-cloud:week2.daily-top-search',
        write_disposition = 'WRITE_APPEND'
    )

    task1 >> storage_to_bigquery >> task2 >> querying_daily_top_search