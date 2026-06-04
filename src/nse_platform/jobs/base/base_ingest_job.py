from nse_platform.jobs.base.base_job import BaseJob
from nse_platform.storage.base_storage import BaseStorage
from nse_platform.context.execution_context import ExecutionContext


class BaseIngestJob(BaseJob):
    """Base for ingest jobs. Adds storage; ETL methods stay abstract."""

    def __init__(self, ec: ExecutionContext, storage: BaseStorage):
        super().__init__(ec)
        self.storage = storage
