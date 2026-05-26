from datetime import datetime
from abc import ABC, abstractmethod

from nse_platform.context.execution_context import ExecutionContext


class BaseJob(ABC):
    def __init__(self, ec: ExecutionContext):
        self.ec = ec

    @abstractmethod
    def extract(self):
        """Get input data. Returns whatever the job needs downstream."""
        ...

    @abstractmethod
    def transform(self, raw):
        """Process the extracted data. Returns transformed data."""
        ...

    @abstractmethod
    def load(self, transformed) -> None:
        """Write the output somewhere."""
        ...

    def run(self) -> None:

        self.ec.logger.info(
            f"{self.ec.job_name} with jobID - {self.ec.run_id} started, dated={self.ec.run_date}")
        try:
            data = self.extract()
            transformed_data = self.transform(data)
            self.load(transformed_data)

        except Exception as e:
            self.ec.logger.error(f"{self.ec.job_name} failed: {e}")
            raise

        finally:
            duration = (datetime.now() -
                        self.ec.started_at).total_seconds()
        self.ec.logger.info(f"{self.ec.job_name} finished in {duration:.2f}s")
