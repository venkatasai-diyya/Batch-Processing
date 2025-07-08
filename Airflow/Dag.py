#daily DAG to run at 6PM EST everyday
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable
from google.cloud import bigquery
from airflow.operators.email import EmailOperator
from datetime import datetime


#Dag configuration
DAG_id = 'customer_overall_daily_run'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
schedule_interval = '0 18 * * *' # 6 PM EST daily

dag = DAG(
    DAG_id,
    default_args=default_args,
    description='Daily DAG to run customer_overall ETL process',
    schedule_interval=schedule_interval,
    catchup=False,
)

# Dataproc cluster configuration
CLUSTER_NAME = 'vzw-dev-analytics-dataproc-cluster'
REGION = 'us-central1'
GCS_BUCKET = 'vzw-dev-analytics-bucket'
PY_FILE_PATH = f'gs://{GCS_BUCKET}/path/to/transform.py'

# Dataproc job configuration
pyspark_job = {
    'reference': {'project_id': 'your-project-id'},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {'main_python_file_uri': PY_FILE_PATH},
}

# Task to submit the PySpark job to Dataproc
daily_run = DataprocSubmitJobOperator(
    task_id='run_transform_py_on_dataproc',
    job=pyspark_job,
    region=REGION,
    dag=dag,
)
#send email when task fails
Email_notification = EmailOperator(
    body='The daily run of the customer_overall ETL process has failed.',
    subject='Daily ETL Process Failure Notification',
    #mail address to send email to
    to='saikirand2898@gmail.com'
    task_id='email_notification',
    dag=dag,
)

#Task dependencies
start >> daily_run >> Email_notification
