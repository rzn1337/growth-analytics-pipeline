from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_aggregation_analytics',
    default_args=DEFAULT_ARGS,
    description='aggregate fct',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_pipeline1 = ExternalTaskSensor(
        task_id='wait_for_pipeline1_success',
        external_dag_id='pipeline_create_fact_wap',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_delta=None,
        mode='poke',
        poke_interval=60,
        timeout=60 * 60 * 3,  # 3 hours
    )

    spark_submit_pipeline2 = SparkSubmitOperator(
        task_id='spark_submit_pipeline2_scala',
        application='/opt/spark/jobs/pipeline2_spark_scala.jar',
        conn_id='spark_default',
        application_args=["--ds", "{{ ds }}"],
        name='pipeline2_spark_job',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '4g',
        },
    )

    wait_for_pipeline1 >> spark_submit_pipeline2