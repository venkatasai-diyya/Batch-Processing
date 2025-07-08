import pandas as pd
from pyhive import hive
from google.cloud import storage as gcs
from google.cloud import bigquery
import os
import logging

#hive setup
hive_hostname = os.getenv('HIVE_HOSTNAME')
hive_port = os.getenv('HIVE_PORT', '8080')
hive_username = os.getenv('HIVE_USERNAME')
hive_password = os.getenv('HIVE_PASSWORD')
hive_database = 'cust_dailytable'
hive_table = 'customer_overall'

# BigQuery setup
bq_project_id = 'vzw-dev-analytics'
bq_dataset_id = 'customer_dev'
bq_table_id = 'customer_overall'


#Hive connection
def get_hive_connection():
    try:
        conn = hive.Connection(
            host=hive_hostname,
            port=int(hive_port),
            username=hive_username,
            password=hive_password,
            database=hive_database
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Hive: {e}")
        raise

#Query data from Hive
query = f"SELECT * FROM {hive_database}.{hive_table}"

df= pd.read_sql(query, get_hive_connection())

#transformation before loading into BigQuery
def transform_data(df):
    #Convert all column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    #making sure rows do not have null values
    df = df.dropna(how='all')
    #Adding a new column with the current timestamp
    df['load_timestamp'] = pd.Timestamp.now()
    #Adding new column named date with the current date
    df['date'] = pd.Timestamp.now().date()
    #Schema evaluation
    df = df.convert_dtypes()
    })
    return df

# Load data into BigQuery
def load_to_bigquery(df, bq_project_id, bq_dataset_id, bq_table_id):
    try:
        table_id = f"{bq_project_id}:{bq_dataset_id}.{bq_table_id}"
        df.write \
            .format("ORC") \
            .option("table", table_id) \
            .option("writeDisposition", "WRITE_APPEND") \
            .save()
        logging.info(f"Loaded data into BigQuery table {table_id}.")
    except Exception as e:
        logging.error(f"Error loading data to BigQuery: {e}")
        raise

# Main ETL process
if __name__ == "__main__":
    hive_database = "cust_dailytable"
    hive_table = "customer_overall"
    bq_project_id = 'vzw-dev-analytics'
    bq_dataset_id = 'customer_dev'
    bq_table_id = 'customer_overall'

    # Query Hive table
    hive_df = query_hive_table(hive_database, hive_table)

    # Transform data
    transformed_df = transform_data(hive_df)

    # Load data into BigQuery
    load_to_bigquery(transformed_df, bq_project_id, bq_dataset_id, bq_table_id)


