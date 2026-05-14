import logging
import uuid
from datetime import datetime, date
from dataclasses import dataclass, field

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
        )
