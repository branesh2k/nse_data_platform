import logging
import uuid
from datetime import datetime, date
from dataclasses import dataclass, field
import sys
import os

from pyspark.sql import SparkSession

from nse_platform.config.settings import settings
from nse_platform.utils.dates import parse_run_date
from nse_platform.utils.logging import make_logger


@dataclass
class ExecutionContext:
    """Runtime info that every job receives."""

    job_name: str
    run_id: str
    run_date: date
    env: str
    logger: logging.Logger
    started_at: datetime
    args: dict = field(default_factory=dict)
    spark: SparkSession | None = None

    @classmethod
    def for_local(cls, job_name: str, run_date: str, **args) -> "ExecutionContext":
        """Build a context for local development runs."""

        return cls(
            job_name=job_name,
            run_id=str(uuid.uuid4()),
            run_date=parse_run_date(run_date),
            env=settings.ENV,
            logger=make_logger(f"nse_platform.{job_name}"),
            started_at=datetime.now(),
            args=args,
            spark=None
        )

    @classmethod
    def for_local_spark(cls, job_name: str, run_date: str, **args) -> "ExecutionContext":
        """Build a context With a local Spark session."""
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        builder = (
            SparkSession.builder
            .appName(job_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/opt/airflow/data/spark-events")
        )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return cls(
            job_name=job_name,
            run_id=str(uuid.uuid4()),
            run_date=parse_run_date(run_date),
            env=settings.ENV,
            logger=make_logger(f"nse_platform.{job_name}"),
            started_at=datetime.now(),
            spark=spark,
            args=args,
        )
