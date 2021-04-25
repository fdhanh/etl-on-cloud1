from datetime import datetime, timedelta
from airflow import models
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

project_id = models.Variable.get("project_id") 
src_project_id = models.Variable.get("src_project_id")
bucket_folder = models.Variable.get("bucket_folder")  
key_path = models.Variable.get("key_path") 

def value(col):
    int_value = ['transaction_id', 'transaction_detail_id', 
                'purchase_quantity', 'purchase_amount']
    str_value = ['transaction_number', 'purchase_payment_method', 
                 'purchase_source', 'product_id']
    if col in int_value:
        return 'value.int_value' 
    return 'value.string_value' 

def integrateTransaction(**kwargs):
    ## push data from bigquery another user using credential key

    credentials = service_account.Credentials.from_service_account_file(key_path+"input-key.json")
    client = bigquery.Client(credentials= credentials,project=src_project_id)
    sql = f"""SELECT * 
            FROM pkl-playing-fields.unified_events.event 
            WHERE (event_name = 'purchase_item') and (event_datetime between '{kwargs.get('prev_ds')}' and '{kwargs.get('ds')}') """
    query = client.query(sql)
    results = query.result()
    df = results.to_dataframe()

    #transform data from table result
    columns = ['transaction_id', 'transaction_detail_id', 'transaction_number', 
            'purchase_quantity', 'purchase_amount', 'purchase_payment_method', 
            'purchase_source', 'transaction_datetime',  'product_id', 
            'user_id', 'state', 'city', 'created_at', 'ext_created_at']
    entity = []
    for idx in range(len(df)):
        temp_row_entity = {}
        row = df.iloc[idx]
        event_params = pd.json_normalize(row['event_params']).set_index('key')
        for key in columns[:7]: 
            try:
                temp_row_entity[key] = event_params[value(key)][key]
            except:
                temp_row_entity[key] = None
        temp_row_entity['transaction_datetime'] = row['event_datetime']
        temp_row_entity['user_id'] = row['user_id']
        temp_row_entity['state'] = row['state']
        temp_row_entity['city'] = row['city']
        temp_row_entity['created_at'] = row['created_at']
        temp_row_entity['ext_created_at'] = kwargs.get('ds')
        entity.append(temp_row_entity)

    #push data to your own bigquery
    credentials = service_account.Credentials.from_service_account_file(key_path+"output-key.json")
    pd.DataFrame(entity).to_gbq(destination_table = "week2.transaction-events",
                                project_id = project_id,
                                if_exists = "replace",
                                credentials = credentials)

default_args = {
    "start_date": datetime(2021, 3, 21),
    "end_date": datetime(2021, 3, 29),
    "depends_on_past": True,
}

with models.DAG(
    "task-2-transform-transaction-table",
    default_args = default_args,
    schedule_interval = timedelta(days=3),
) as dag:
    transactional_events = PythonOperator(
        task_id = 'etl-to-bq',
        python_callable = integrateTransaction,
        provide_context = True
    )