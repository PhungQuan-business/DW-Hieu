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

def run_bigquery_query(sql_query, **kwargs):
    hook = BigQueryHook(gcp_conn_id='google_cloud_bigquery_default')
    client = hook.get_client()

    job_config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED'
    )

    query_job = client.query(sql_query, job_config=job_config)
    query_job.result()  # Wait for the job to complete


t1 = PythonOperator(
    task_id='create_update_dim_customer',
    python_callable=run_bigquery_query,
    op_kwargs={'sql_query': DimCustomer_sql_query},
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_update_dim_date',
    python_callable=run_bigquery_query,
    op_kwargs={'sql_query': DimDate_sql_query},
    dag=dag,
)

t3 = PythonOperator(
    task_id='create_update_dim_campaign',
    python_callable=run_bigquery_query,
    op_kwargs={'sql_query': DimCampaign_sql_query},
    dag=dag,
)

t4 = PythonOperator(
    task_id='create_update_fact_table',
    python_callable=run_bigquery_query,
    op_kwargs={'sql_query': FactTable_sql_query},
    dag=dag,
)

task_delay = BashOperator(task_id="delay_bash_task",
                          dag=dag,
                          bash_command="sleep 5s")

[t1, t2, t3] >> task_delay >> t4

if __name__ == "__main__":
    dag.cli()
