from pathlib import Path
from nse_platform.context.execution_context import ExecutionContext
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.jobs.equity.bronze_job import EquityBronzeJob
from nse_platform.config.settings import settings

ec = ExecutionContext.for_local_spark("bronze", "2023-05-24")
storage = LocalStorage(settings.LOCAL_DATA_ROOT)
job = EquityBronzeJob(ec, storage)
job.run()
