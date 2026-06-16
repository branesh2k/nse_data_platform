import argparse

from nse_platform.context.execution_context import ExecutionContext
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.jobs.silver.equity_silver_job import EquitySilverJob
from nse_platform.config.settings import settings


def main():
    parser = argparse.ArgumentParser(description="NSE equity silver")
    parser.add_argument("--run-date", required=True,
                        help="run date, YYYY-MM-DD")
    args = parser.parse_args()

    run_date = args.run_date
    ec = ExecutionContext.for_local_spark("nse_equity_silver", run_date)
    storage = LocalStorage(settings.LOCAL_DATA_ROOT)
    job = EquitySilverJob(ec, storage)
    job.run()


if __name__ == "__main__":
    main()
