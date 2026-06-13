import sys

from nse_platform.config.settings import settings
from nse_platform.context.execution_context import ExecutionContext
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.sources.nse_client import NseClient
from nse_platform.jobs.ingest.equity_ingest import NseBhavcopyIngestJob


def main():
    run_date = sys.argv[1]
    ec = ExecutionContext.for_local("nse_bhavcopy_ingest", run_date)
    storage = LocalStorage(settings.LOCAL_DATA_ROOT)
    nse_client = NseClient(
        settings.NSE_BASE_URL,
        settings.UDIFF_CUTOVER_DATE,
        logger=ec.logger,
    )
    job = NseBhavcopyIngestJob(ec, storage, nse_client)
    job.run()


if __name__ == "__main__":
    main()
