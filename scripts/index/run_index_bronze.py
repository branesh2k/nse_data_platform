import argparse

from nse_platform.context.execution_context import ExecutionContext
from nse_platform.jobs.bronze.index_bronze_job import IndexBronzeJob
from nse_platform.storage.factory import make_storage
from nse_platform.config.settings import settings


def main():
    parser = argparse.ArgumentParser(description="NSE Index bronze")
    parser.add_argument("--run-date", required=True,
                        help="run date, YYYY-MM-DD")
    args = parser.parse_args()

    run_date = args.run_date

    ec = ExecutionContext.for_local_spark("index_bronze", run_date)
    storage = make_storage()
    job = IndexBronzeJob(ec, storage)
    job.run()


if __name__ == "__main__":
    main()
