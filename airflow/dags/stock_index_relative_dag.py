from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="stock_index_relative_dag",
    start_date=datetime(2026, 5, 1),
    schedule="0 18 * * 1-5",
    catchup=False,
    max_active_runs=1,
)
def stock_index_relative_dag():

    @task.bash
    def equity_ingest():
        return "python /opt/airflow/scripts/equity/run_nse_equity_ingest.py --run-date {{ ds }}"

    @task.bash
    def equity_bronze():
        return "python /opt/airflow/scripts/equity/run_equity_bronze.py --run-date {{ ds }}"
        # return "echo JAVA_HOME=$JAVA_HOME && echo PATH=$PATH && python /opt/airflow/scripts/equity/run_equity_bronze.py --run-date {{ ds }}"

    @task.bash
    def equity_silver():
        return "python /opt/airflow/scripts/equity/run_equity_silver.py --run-date {{ ds }}"

    @task.bash
    def index_ingest():
        return "python /opt/airflow/scripts/index/run_nse_index_ingest.py --run-date {{ ds }}"

    @task.bash
    def index_bronze():
        return "python /opt/airflow/scripts/index/run_index_bronze.py --run-date {{ ds }}"

    @task.bash
    def index_silver():
        return "python /opt/airflow/scripts/index/run_index_silver.py --run-date {{ ds }}"

    @task.bash
    def gold():
        return "python /opt/airflow/scripts/run_stock_vs_market_gold.py --run-date {{ ds }}"

    [equity_ingest() >> equity_bronze() >> equity_silver(),
     index_ingest() >> index_bronze() >> index_silver()] >> gold()


stock_index_relative_dag()
