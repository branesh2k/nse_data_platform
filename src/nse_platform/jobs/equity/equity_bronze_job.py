from pathlib import Path

from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.utils.path_builder import bronze_bhavcopy_path, filename_for, raw_bhavcopy_key
from nse_platform.utils.zip_utils import unzip_single_csv


class BronzeJob(BaseSparkJob):
    def __init__(self, ec, storage):
        super().__init__(ec, storage)
        self.tmp_dir = Path("./data/_tmp")
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_path = self.tmp_dir / f"{self.ec.run_id}.csv"

    def extract(self):
        filename = filename_for(self.ec.run_date, settings.UDIFF_CUTOVER_DATE)
        path = raw_bhavcopy_key(self.ec.run_date, filename)
        zip_bytes = self.storage.get(path)
        csv_bytes = unzip_single_csv(zip_bytes)

        self.tmp_path.write_bytes(csv_bytes)
        self.ec.logger.info(f"unzipped csv written to {self.tmp_path}")

        raw_df = self.spark.read.csv(
            str(self.tmp_path), header=True)
        return raw_df

    def transform(self, raw):
        return raw

    def load(self, transformed):
        output_path = bronze_bhavcopy_path(self.ec.run_date)
        try:
            transformed.write.mode("overwrite").parquet(output_path)
            self.ec.logger.info(f"data loaded as parquet in {output_path}")
        finally:
            if self.tmp_path.exists():
                self.tmp_path.unlink()
                self.ec.logger.info(f"deleted temp file {self.tmp_path}")
