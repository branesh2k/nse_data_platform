
import sys

from nse_platform.config.settings import settings
from nse_platform.context.execution_context import ExecutionContext
from nse_platform.jobs.index.ingest import NseIndexIngestJob
from nse_platform.sources.nse_client import NseClient
from nse_platform.storage.local_storage import LocalStorage


def main():
    run_date = sys.argv[1]
    ec = ExecutionContext.for_local("nse_index_ingest", run_date)
    storage = LocalStorage(settings.LOCAL_DATA_ROOT)
    client = NseClient(base_url=settings.NSE_BASE_URL,
                       udiff_cutover=settings.UDIFF_CUTOVER_DATE, logger=ec.logger,)
    job = NseIndexIngestJob(ec, storage, client)
    job.run()


if __name__ == "__main__":
    main()
