from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import io

# Default Args
default_args = {
    'owner': 'ahmed-refat',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# S3 Settings
BUCKET = 'bronze-financial-data-ahmed'

# Data Quality Function
def run_data_quality():
    s3 = boto3.client('s3', region_name='us-east-1')
    errors = []

    for file in ['fraudTrain.csv', 'fraudTest.csv']:
        obj = s3.get_object(Bucket=BUCKET, Key=f'raw/{file}')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))

        # Checks
        for col in ['amt', 'trans_date_trans_time', 'merchant', 'is_fraud']:
            if df[col].isnull().sum() > 0:

        if (df['amt'] <= 0).sum() > 0:
            errors.append(f" في amounts سالبة في {file}")

        if df.duplicated().sum() > 0:
            errors.append(f" في duplicates في {file}")

    if errors:
        raise Exception(f"Data Quality Failed: {errors}")
    
    print(" كل الـ Quality Checks عدت صح!")

# DAG
with DAG(
    dag_id='financial_pipeline',
    default_args=default_args,
    description='Financial Data Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 4),
    catchup=False,
    tags=['financial', 'etl'],
) as dag:

    # Task 1: Data Quality
    data_quality = PythonOperator(
        task_id='data_quality_check',
        python_callable=run_data_quality,
    )

    # Task 2: Glue Job
    glue_job = GlueJobOperator(
        task_id='run_glue_job',
        job_name='financial-processing-job',
        region_name='us-east-1',
        aws_conn_id='aws_default',
        wait_for_completion=True,
    )

    # Task 3: dbt Run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dags/financial_dbt && dbt run --profiles-dir /opt/airflow/dags/.dbt',
    )

    # Task 4: dbt Test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dags/financial_dbt && dbt test --profiles-dir /opt/airflow/dags/.dbt',
    )

    # Flow
    data_quality >> glue_job >> dbt_run >> dbt_test
