from nse_platform.context.execution_context import ExecutionContext
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.jobs.equity.equity_silver_job import SilverJob
from nse_platform.config.settings import settings

ec = ExecutionContext.for_local_spark("silver", "2026-01-01")
storage = LocalStorage(settings.LOCAL_DATA_ROOT)
job = SilverJob(ec, storage)
job.run()
