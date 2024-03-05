# Booking data

- The process start from the index.py file. The function download_from_source() will accept arguments repo_url and file_paths which provides the github url and file paths respectively.
- The transform_data() will accept each dataframes and transform the fields accordingly.
- insert_to_psql() function will create tables based on the dataframes and insert it into a psql database
- Finallly, upload_dataframe_to_gcs() will uploads the tables to a GCS cloud storage bucket.

To ensure the security of database and GCS credentials we can make use of **GCS secret manager service**. The credentials can be saved as secrets in the secret manager and can be downloaded as json to use. 



The pipeline can be hosted as a workflow since it resembles an **ETL process**. Where we **extract** data from the CSV files from a source (here GitHub), **transform** it using appropriate data analytical tools(eg pandas) and **load** it into a destination (here, Postgres and GCS cloud storage). 

To schedule this workflow we can make use of **Apache Airflow**(a platform created to programmatically author, schedule and monitor workflows.), which will convert this process into a DAG, which can be run accroding to a schedule( eg: daily, weekly or hourly). 

In this pipeline since we are using GCS cloud we can make use of the **Cloud Composer service** provided by GCS which is a fully managed workflow orchestration service built on Apache Airflow.
