
import argparse

from nse_platform.config.settings import settings
from nse_platform.context.execution_context import ExecutionContext
from nse_platform.jobs.ingest.index_ingest import NseIndexIngestJob
from nse_platform.sources.nse_client import NseClient
from nse_platform.storage.local_storage import LocalStorage


def main():
    parser = argparse.ArgumentParser(description="NSE Index Ingest")
    parser.add_argument("--run-date", required=True,
                        help="run date, YYYY-MM-DD")
    args = parser.parse_args()

    run_date = args.run_date
    ec = ExecutionContext.for_local(
        "nse_index_ingest", run_date)
    storage = LocalStorage(settings.LOCAL_DATA_ROOT)
    client = NseClient(base_url=settings.NSE_BASE_URL,
                       udiff_cutover=settings.UDIFF_CUTOVER_DATE, logger=ec.logger,)
    job = NseIndexIngestJob(ec, storage, client)
    job.run()


if __name__ == "__main__":
    main()
