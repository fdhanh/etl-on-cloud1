# ETL on cloud with Google Cloud Platform environment

## Case 1
How to pull data from cloud storage, move it into BigQuery, and looking for what was the most search keyword of the day, and run composer for daily schedule.
Tools: Cloud Storage, BigQuery, Cloud Composer, and Dataflow.

![alt text](https://github.com/fdhanh/etl-on-cloud1/blob/master/add_files/task-1.JPG?raw=true)

## Case 2
How to pull data from another BigQuery table from another account using credential, take some field as it need, store into your own BigQuery table, and run composer for 3 days interval schedule.
Tools: BigQuery, Cloud Composer.

![alt text](https://github.com/fdhanh/etl-on-cloud1/blob/master/add_files/task-2.JPG?raw=true)

# Installation
Use git to clone this repository

`git clone https://github.com/fdhanh/etl-on-cloud1.git`

# Prerequisite

`Python 3.7.3`

To run the script in this repository, you need to install the prerequisite library from requirements.txt

`pip install -r requirements.txt`

# Usage
1. Enable API(for Cloud Dataflow and Cloud Composer)
2. Create service account as an owner
3. Create bucket and bq table or send this code to shell `./etl-on-cloud1/main.sh`
4. Create cloud composer
	follow this environment
	- location: us-central1
	- node count: 3
	- zone: us-central1-a
	- machine type: n1-standard-1
	- disk size: 20
	- service account: adjust to the service account that you just created
5. If the composer was done created, go to airflow UI. Create variable on admin page. 
	You can upload file on folder `bucket/variables.json` in this repo and dont forget to fit your project id, etc.
6. Upload dags files to `dags/` folder <br>
	Or you can send this code to shell:
	`gsutil cp etl-on-cloud1/dags/* gs://<your composer bucket>/dags`
	And upload your json credential files to `data/` folder
7. Check your airflow task.