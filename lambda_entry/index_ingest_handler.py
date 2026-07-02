import os


def handler(event, context):
    os.environ["DATA_ROOT"] = event["data_root"]
    os.environ["STORAGE_BACKEND"] = event["storage_backend"]

    from nse_platform.context.execution_context import ExecutionContext
    from nse_platform.storage.factory import make_storage
    from nse_platform.sources.nse_client import NseClient
    from nse_platform.jobs.ingest.index_ingest import NseIndexIngestJob

    from nse_platform.config.settings import settings

    ec = ExecutionContext.for_local("nse_index_ingest", event["run_date"])
    storage = make_storage()
    client = NseClient(settings.NSE_BASE_URL, settings.UDIFF_CUTOVER_DATE)
    NseIndexIngestJob(ec, storage, client).run()

    return {"status": "ok", "run_date": event["run_date"]}
