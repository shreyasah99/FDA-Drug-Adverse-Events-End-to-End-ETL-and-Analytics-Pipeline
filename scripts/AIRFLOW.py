from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.S3_hook import S3Hook
import requests
import json
import pandas as pd
import logging
from io import StringIO
import boto3
from etlfunctions import extract_data, transform_data




##Creating snowflake stage
def create_stage():
    aws_hook = S3Hook(aws_conn_id='aws-default')
    credentials = aws_hook.get_credentials()
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key

    create_stage_sql = f"""
    CREATE OR REPLACE STAGE FDA_DB.FDA_STAGES.fda_stage    
    URL='s3://fda-data-airflow-etl-project/api-cleaned-data/'
    CREDENTIALS = (AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
    FILE_FORMAT = (TYPE = CSV
                    SKIP_HEADER = 1
                    FIELD_DELIMITER = ',');
    """

    return create_stage_sql


# Function to upload DataFrame to S3 and push S3 path to XCom
def upload_df_to_s3_and_push_to_xcom(**kwargs):
    
    df = extract_data()
    # Convert DataFrame to CSV string
    csv_data = df.to_csv(index=False)

    s3_bucket = 'fda-data-airflow-etl-project'
    s3_key = 'api-extracted-data/FDA_data.csv'
    s3_hook = S3Hook(aws_conn_id='aws-default')

    # Upload DataFrame to S3
    s3_hook.load_string(csv_data, key=s3_key, bucket_name=s3_bucket)
    s3_path = f's3://{s3_bucket}/{s3_key}'
    # Push S3 path to XCom
    kwargs['ti'].xcom_push(key='s3_path', value=s3_path)


#function to extract S3 path from Xcom and reteive raw data from s3
def pull_from_xcom_and_pull_data_from_s3(**kwargs):
    
    # Retrieve the S3 path from XCom
    s3_path = kwargs['ti'].xcom_pull(task_ids='upload_to_s3_and_push_to_xcom', key='s3_path')

    # Extract bucket_name and s3_key from the pulled path
    s3_hook = S3Hook(aws_conn_id='aws-default')
    bucket_name, s3_key = s3_hook.parse_s3_url(s3_path)
    obj = s3_hook.get_key(s3_key, bucket_name)

    # Read data from S3 object into a DataFrame
    data = obj.get()['Body'].read().decode('utf-8')
    data = pd.read_csv(StringIO(data))
    
    reports, patients, symptoms, drugs  = transform_data(data)
    
    reports_data = reports.to_csv(index=False)
    patients_data = patients.to_csv(index=False)
    symptoms_data = symptoms.to_csv(index=False)
    drugs_data = drugs.to_csv(index=False)

    ##loading transformed df into s3
    
    s3_bucket= 'fda-data-airflow-etl-project'
    s3_key_reports = 'api-cleaned-data/reports.csv'
    s3_key_patients = 'api-cleaned-data/patients.csv'
    s3_key_symptoms = 'api-cleaned-data/symptoms.csv'
    s3_key_drugs= 'api-cleaned-data/drugs.csv'
    

    s3_hook.load_string(reports_data, key=s3_key_reports, bucket_name=s3_bucket)
    s3_hook.load_string(patients_data, key=s3_key_patients, bucket_name=s3_bucket)
    s3_hook.load_string(symptoms_data, key=s3_key_symptoms, bucket_name=s3_bucket)
    s3_hook.load_string(drugs_data, key=s3_key_drugs, bucket_name=s3_bucket)



# Initialize DAG deafult parameter
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    
    
}

# Define the DAG
with DAG(
    'FDA_DATA_ETL',
    default_args=default_args,
    description='EXTRACT FDA data from API, push to S3, pull from S3 and TRANSFORM, LOAD into snowflake',
    schedule_interval=None,
    catchup = False) as dag:
    
# Tash to check if the api is ready
        
        is_FDA_api_ready = HttpSensor(
                task_id ='is_FDA_API_ready',
                http_conn_id='http-api',
                endpoint='',
                )
        
# Task to extract and upload API data to S3 and push S3 path to XCom fro reference
        upload_to_s3_task = PythonOperator(
            task_id='upload_to_s3_and_push_to_xcom',
            python_callable=upload_df_to_s3_and_push_to_xcom,
            dag=dag,
            
            )

# Task to pull API data to S3 using XCom pull for S3 path reference
        load_from_s3_task = PythonOperator(
            task_id='load_from_s3_using_s3reference_from_xcom',
            python_callable=pull_from_xcom_and_pull_data_from_s3,
            dag=dag,
        )

#Task to create snowflake Stage to load data into tables
        create_snowflake_stage = SnowflakeOperator(
            task_id='create_snowflake_stage',
            snowflake_conn_id='snowflake_conn_id',
            sql=create_stage(),
            dag=dag,
        )
    
#task to copy data from the stage into snowflake tables table
        copy_FDA_data_into_tables = SnowflakeOperator(
        task_id='copy_into_snowflake_table',
        snowflake_conn_id='snowflake_conn_id',
        sql="""

        COPY INTO FDA_DB.PUBLIC.fda_reports
        FROM @FDA_DB.FDA_STAGES.fda_stage
        FILES= ('reports.csv')
        ON_ERROR = 'CONTINUE';

        COPY INTO FDA_DB.PUBLIC.fda_patients
        FROM @FDA_DB.FDA_STAGES.fda_stage
        FILES= ('patients.csv')
        ON_ERROR = 'CONTINUE';

        COPY INTO FDA_DB.PUBLIC.fda_symptoms
        FROM @FDA_DB.FDA_STAGES.fda_stage
        FILES= ('symptoms.csv')
        ON_ERROR = 'CONTINUE';

        COPY INTO FDA_DB.PUBLIC.fda_drugs
        FROM @FDA_DB.FDA_STAGES.fda_stage
        FILES= ('drugs.csv')
        ON_ERROR = 'CONTINUE';

        """,
        dag=dag,
    )





# task dependencies
        is_FDA_api_ready >> upload_to_s3_task >> load_from_s3_task >> create_snowflake_stage >> copy_FDA_data_into_tables