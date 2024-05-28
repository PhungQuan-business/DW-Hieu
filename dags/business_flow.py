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
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from datetime import timedelta

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/DW_service_account.json'

default_args = {
    'owner': 'Hieu',
    'start_date': None,  # YYYY-MM-DD
    'retries': None,
    'retry_daylsy': None
}

dag = DAG(
    'OLTP_to_OLAP_with_Hook',
    default_args=default_args,
    description='Run BigQuery SQL query',
    schedule=None,
)

# ------------Read Query-------------#


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()


DimCustomer_sql_query = read_sql_file('/opt/airflow/dags/SQL/dim_customer.sql')
DimCampaign_sql_query = read_sql_file('/opt/airflow/dags/SQL/dim_campaign.sql')
DimDate_sql_query = read_sql_file('/opt/airflow/dags/SQL/dim_date.sql')
FactTable_sql_query = read_sql_file('/opt/airflow/dags/SQL/fact_table.sql')


# ------------Airflow Operators-------------#

t1 = BigQueryInsertJobOperator(
    task_id='create_update_dim_customer',
    configuration={
        "query": {
            "query": DimCustomer_sql_query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id='google_cloud_bigquery_default',
    dag=dag,
)

t2 = BigQueryInsertJobOperator(
    task_id='create_update_dim_date',
    configuration={
        "query": {
            "query": DimDate_sql_query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id='google_cloud_bigquery_default',
    dag=dag,
)

t3 = BigQueryInsertJobOperator(
    task_id='create_update_dim_campaign',
    configuration={
        "query": {
            "query": DimCampaign_sql_query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id='google_cloud_bigquery_default',
    dag=dag,
)

t4 = BigQueryInsertJobOperator(
    task_id='create_update_fact_table',
    configuration={
        "query": {
            "query": FactTable_sql_query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id='google_cloud_bigquery_default',
    dag=dag,
)


task_delay = BashOperator(task_id="delay_bash_task",
                          dag=dag,
                          bash_command="sleep 5s")

[t1, t2, t3] >> task_delay >> t4

if __name__ == "__main__":
    dag.cli()
