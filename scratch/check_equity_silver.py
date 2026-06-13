from nse_platform.context.execution_context import ExecutionContext
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.jobs.silver.equity_silver_job import EquitySilverJob
from nse_platform.config.settings import settings

ec = ExecutionContext.for_local_spark("silver", "2023-05-23")
storage = LocalStorage(settings.LOCAL_DATA_ROOT)
job = EquitySilverJob(ec, storage)
job.run()
