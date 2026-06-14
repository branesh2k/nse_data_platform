from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="stock_index_relative_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def stock_index_relative_dag():

    @task
    def equity_ingest():
        print("equity_ingest")
    @task
    def equity_bronze():
        print("equity_bronze")
    @task
    def equity_silver():
        print("equity_silver")

    @task
    def index_ingest():
        print("index_ingest")

    @task
    def index_bronze():
        print("index_bronze")

    @task
    def index_silver():
        print("index_silver")

    @task
    def gold():
        print("gold")

    
    [equity_ingest() >> equity_bronze() >> equity_silver(), index_ingest() >> index_bronze() >> index_silver()] >> gold()

stock_index_relative_dag()
