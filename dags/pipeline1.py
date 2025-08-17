from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_create_fact_wap',
    default_args=DEFAULT_ARGS,
    description='populate fct using WAP',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    spark_submit_pipeline1 = SparkSubmitOperator(
        task_id='spark_submit_pipeline1_py',
        application='/opt/spark/jobs/pipeline1_spark_py.py',
        conn_id='spark_default',
        application_args=[
            "--ds", "{{ ds }}",
            "--jdbc_url", "{{ var.value.postgres_jdbc_url or 'jdbc:postgresql://localhost:5432/postgres' }}",
            "--jdbc_user", "{{ var.value.postgres_user or 'postgres' }}",
            "--jdbc_password", "{{ var.value.postgres_password or 'postgres' }}",
            "--jdbc_schema", "{{ var.value.postgres_schema or 'public' }}",
        ],
        name='pipeline1_spark_job',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '4g',
        },
        dag=dag,
    )

    spark_submit_pipeline1