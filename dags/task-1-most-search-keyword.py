from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

bucket_input = models.Variable.get("bucket_input")
bucket_folder = models.Variable.get("bucket_folder")
project_id = models.Variable.get("project_id") 
gce_region = models.Variable.get("gce_region") 
gce_zone = models.Variable.get("gce_zone") 
output_full_table = models.Variable.get("task_1_schema_table_output_full")
output_analyze_table = models.Variable.get("task_1_schema_table_output_output")

default_args = {
    "start_date": datetime(2021,3,10),
    "end_date"  : datetime(2021,3,15), 
    "dataflow_default_options": {
        "project": project_id,  
        "region": gce_region, 
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_folder + "/tmp/",
        "numWorkers": 1,
    },
}

# Define a DAG (directed acyclic graph) of tasks.
with models.DAG(
    # The id you will see in the DAG airflow page
    "task-1-most-search-keyword",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=timedelta(days=1),  # Override to match your needs
) as dag:
    
    storage_to_bigquery = DataflowTemplateOperator(
        # The task id of your job
        task_id="storage_to_bigquery",
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        # Use the link above to specify the correct parameters for your template.
        parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath": bucket_folder + "/jsonSchema.json",
            "javascriptTextTransformGcsPath": bucket_folder + "/transform.js",
            "inputFilePattern": bucket_input + "/search_{{ ds_nodash }}.csv",
            "outputTable": output_full_table,
            "bigQueryLoadingTemporaryDirectory": bucket_folder + "/tmp/",
        },
    )  

    querying_daily_top_search = BigQueryOperator(
        task_id = "querying_daily_top_search",
        sql =  """SELECT lower(search_keyword) as keyword, count(lower(search_keyword)) as search_count, created_at as date
               FROM `etl-on-cloud.week2.search-keyword`
               WHERE created_at = '{{ds}}'
               GROUP BY keyword, created_at
               ORDER BY search_count desc
               LIMIT 1;""",
        use_legacy_sql = False,
        destination_dataset_table = output_analyze_table,
        write_disposition = 'WRITE_APPEND'
    )

    storage_to_bigquery >> querying_daily_top_search
