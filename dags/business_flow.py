import airflow
from airflow.models.dag import DAG
from datetime import timedelta, datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage, bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/DW-service-account.json'
# os.environ['AIRFLOW_UID'] = '50000'

default_args = {
    'owner': 'Hieu',
    # 'start_date': airflow.utils.dates.days_ago(0), # chưa hiểu function này
    'start_date': None,  # YYYY-MM-DD
    'retries': None,
    'retry_daylsy': None
}

dag = DAG(
    'OLTP_to_OLAP',
    default_args=default_args,
    description='Run BigQuery SQL query',
    schedule=None,
)

# ------------Read Query-------------#


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()


DimCustomer_sql_query = read_sql_file('/opt/airflow/dags/SQL/dim_customer.sql')
DimCampaign_sql_query = read_sql_file(
    '/opt/airflow/dags/SQL/dim_campaign.sql')
DimDate_sql_query = read_sql_file('/opt/airflow/dags/SQL/dim_date.sql')
FactTable_sql_query = read_sql_file('/opt/airflow/dags/SQL/fact_table.sql')


# ------------Airflow Operators-------------#

def print_env_vars():
    print("Environment Variables:")
    for key, value in os.environ.items():
        print(f"{key}: {value}")


print_env_task = PythonOperator(
    task_id='print_env_vars',
    python_callable=print_env_vars,
    dag=dag,
)

t1 = BigQueryInsertJobOperator(
    task_id='create_update_dim_customer',
    configuration={
        "query": {
            "query": DimCustomer_sql_query,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED"
        }
    },
    gcp_conn_id='my_gcp_conn',
    dag=dag,
)

t2 = BigQueryInsertJobOperator(
    task_id='create_update_dim_date',
    configuration={
        "query": {
            "query": DimDate_sql_query,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED"
        }
    },
    gcp_conn_id='my_gcp_conn',
    dag=dag,
)

t3 = BigQueryInsertJobOperator(
    task_id='create_update_dim_campaign',
    configuration={
        "query": {
            "query": DimCampaign_sql_query,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED"
        }
    },
    gcp_conn_id='my_gcp_conn',
    dag=dag,
)

t4 = BigQueryInsertJobOperator(
    task_id='create_update_fact_table',
    configuration={
        "query": {
            "query": FactTable_sql_query,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED"
        }
    },
    gcp_conn_id='my_gcp_conn',
    dag=dag,
)

TaskDelay = BashOperator(task_id="delay_bash_task",
                         dag=dag,
                         bash_command="sleep 5s")


print_env_task >> [t1, t2, t3] >> TaskDelay >> t4

if __name__ == "__main__":
    dag.cli()
