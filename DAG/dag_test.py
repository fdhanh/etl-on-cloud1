

"""Example Airflow DAG that creates a Cloud Dataflow workflow which takes a
text file and adds the rows to a BigQuery table.

This DAG relies on four Airflow variables
https://airflow.apache.org/concepts.html#variables
* project_id - Google Cloud Project ID to use for the Cloud Dataflow cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataflow cluster should be
  created.
* gce_region - Google Compute Engine region where Cloud Dataflow cluster should be
  created.
Learn more about the difference between the two here:
https://cloud.google.com/compute/docs/regions-zones
* bucket_path - Google Cloud Storage bucket where you've stored the User Defined
Function (.js), the input file (.txt), and the JSON schema (.json).
"""

import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

bucket_input = models.Variable.get("bucket_input")
bucket_folder = models.Variable.get("bucket_folder")
project_id = models.Variable.get("project_id") 


default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id, 
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_folder + "/tmp/",
    },
}

def get_date(**kwargs):
   return kwargs.get('ds-nodash')

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_dataflow_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:
    
    storage_to_bigquey = DataflowTemplateOperator(
    # The task id of your job
    task_id="storage_to_bigquey",
    template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
    # Use the link above to specify the correct parameters for your template.
    parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath": bucket_folder + "/jsonSchema.json",
            "javascriptTextTransformGcsPath": bucket_folder + "/transform.js",
            "inputFilePattern": bucket_input + f"/search_{get_date()}.csv",
            "outputTable": project_id + ":week2.search-keyword",
            "bigQueryLoadingTemporaryDirectory": bucket_folder + "/tmp/",
        },
    )

    task1 = PythonOperator(
        task_id = "trial",
        python_callable = get_date,
        provide_context = True)
    
    storage_to_bigquey >> task1