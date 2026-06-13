from nse_platform.context.execution_context import ExecutionContext
from nse_platform.jobs.gold.stock_vs_market_gold_job import StockVsMarketGoldJob
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.config.settings import settings

ec = ExecutionContext.for_local_spark("gold", "2026-01-01")
storage = LocalStorage(settings.LOCAL_DATA_ROOT)
job = StockVsMarketGoldJob(ec, storage)
job.run()
