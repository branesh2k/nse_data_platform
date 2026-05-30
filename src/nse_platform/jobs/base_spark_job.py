from pyspark.sql import SparkSession

from nse_platform.jobs.base_job import BaseJob
from nse_platform.storage.base_storage import BaseStorage
from nse_platform.context.execution_context import ExecutionContext


class BaseSparkJob(BaseJob):
    """Base for Spark jobs (bronze/silver/gold). Requires a Spark-enabled context."""

    def __init__(self, ec: ExecutionContext, storage: BaseStorage):
        super().__init__(ec)
        if ec.spark is None:
            raise ValueError(
                "BaseSparkJob requires a Spark session. "
                "Build the context with ExecutionContext.for_local_spark()."
            )
        self.storage = storage
        self.spark: SparkSession = ec.spark
