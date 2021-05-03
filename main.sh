gsutil mb gs://week2-bucket-folder

gsutil mb gs://search-keyword-dataset

gsutil cp etl-on-cloud1/dataset/*.csv gs://search-keyword-dataset

gsutil cp etl-on-cloud1/bucket/* gs://week2-bucket-folder

bq --location=US mk \
--dataset \ 
week2 #dataset name