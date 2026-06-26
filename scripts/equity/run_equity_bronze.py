import argparse
from nse_platform.context.execution_context import ExecutionContext
from nse_platform.storage.factory import make_storage
from nse_platform.jobs.bronze.equity_bronze_job import EquityBronzeJob
from nse_platform.config.settings import settings


def main():
    parser = argparse.ArgumentParser(description="NSE equity bronze")
    parser.add_argument("--run-date", required=True,
                        help="run date, YYYY-MM-DD")
    args = parser.parse_args()

    run_date = args.run_date
    ec = ExecutionContext.for_local_spark("nse_equity_bronze", run_date)
    storage = make_storage()
    job = EquityBronzeJob(ec, storage)
    job.run()


if __name__ == "__main__":
    main()
