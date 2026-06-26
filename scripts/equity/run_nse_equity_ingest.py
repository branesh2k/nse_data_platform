import argparse

from nse_platform.config.settings import settings
from nse_platform.context.execution_context import ExecutionContext
from nse_platform.storage.factory import make_storage
from nse_platform.sources.nse_client import NseClient
from nse_platform.jobs.ingest.equity_ingest import NseBhavcopyIngestJob


def main():
    parser = argparse.ArgumentParser(description="NSE equity bhavcopy ingest")
    parser.add_argument("--run-date", required=True,
                        help="run date, YYYY-MM-DD")
    args = parser.parse_args()

    run_date = args.run_date
    ec = ExecutionContext.for_local("nse_bhavcopy_ingest", run_date)
    storage = make_storage()
    nse_client = NseClient(
        settings.NSE_BASE_URL,
        settings.UDIFF_CUTOVER_DATE,
        logger=ec.logger,
    )
    job = NseBhavcopyIngestJob(ec, storage, nse_client)
    job.run()


if __name__ == "__main__":
    main()
