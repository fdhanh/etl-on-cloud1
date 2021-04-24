# ETL on cloud with Google Cloud Platform environment

## Case 1
How to pull data from cloud storage, move it into bigquery, and looking for what was the most search of the day. 
Tools: Cloud Storage, BigQuery, Cloud Composer, and Dataflow

## Case 2

# Installation
Use git to clone this repository
`git clone https://github.com/fdhanh/etl-on-cloud1.git`

# Prerequisite

`Python 3.7.3`

To run the script in this repository, you need to install the prerequisite library from requirements.txt
`pip install -r requirements.txt`

# Usage
Before you start, you need to:
- Enable dataflow and composer API's.
- Create new service account as a owner.
- Create storage bucket
- Move dataset files and file for dataflow template to storage bucket

## Airflow
Before start airflow, you need to create variables. Variables can be retrieved in folder `dags\task-1` and upload dags file to composer dag folder
