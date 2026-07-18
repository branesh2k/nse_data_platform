from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)

REGION = "us-east-1"


@dag(
    dag_id="stock_index_relative_dag",
    start_date=datetime(2026, 5, 1),
    schedule="0 18 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(seconds=30)},
)
def stock_index_relative_dag():

    equity_ingest = LambdaInvokeFunctionOperator(
        task_id="equity_ingest",
        function_name="nse-equity-ingest-Function",
        payload='{"run_date": "{{ ds }}", "data_root": "s3://nse-platform", "storage_backend": "s3"}',
        region_name=REGION,
    )

    equity_bronze = GlueJobOperator(
        task_id="equity_bronze",
        job_name="bronzeEquity",
        script_args={"--run_date": "{{ ds }}"},
        region_name=REGION,
    )

    equity_silver = GlueJobOperator(
        task_id="equity_silver",
        job_name="silverEquity",
        script_args={"--run_date": "{{ ds }}"},
        region_name=REGION,
    )

    index_ingest = LambdaInvokeFunctionOperator(
        task_id="index_ingest",
        function_name="nse-index-ingest-Function",
        payload='{"run_date": "{{ ds }}", "data_root": "s3://nse-platform", "storage_backend": "s3"}',
        region_name=REGION,
    )

    index_bronze = GlueJobOperator(
        task_id="index_bronze",
        job_name="BronzeIndex",
        script_args={"--run_date": "{{ ds }}"},
        region_name=REGION,
    )

    index_silver = GlueJobOperator(
        task_id="index_silver",
        job_name="silverIndex",
        script_args={"--run_date": "{{ ds }}"},
        region_name=REGION,
    )

    gold = GlueJobOperator(
        task_id="gold",
        job_name="goldJob",
        script_args={"--run_date": "{{ ds }}"},
        region_name=REGION,
    )

    [equity_ingest >> equity_bronze >> equity_silver,
     index_ingest >> index_bronze >> index_silver] >> gold


stock_index_relative_dag()
