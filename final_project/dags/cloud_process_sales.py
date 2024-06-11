from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

BUCKET_NAME = "de-07-kondratiuk-final-bucket"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}


with DAG(
    'cloud_process_sales',
    default_args=default_args,
    description='Load data from GCS to BigQuery',
    schedule_interval=None,
) as dag:

    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket=BUCKET_NAME,
        prefix='data/sales/',
        delimiter=None,
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=list_files.output,
        destination_project_dataset_table='de-07-denys-kondratiuk.bronze.sales',
        project_id='de-07-denys-kondratiuk',
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag,
    )

    list_files >> gcs_to_bigquery

