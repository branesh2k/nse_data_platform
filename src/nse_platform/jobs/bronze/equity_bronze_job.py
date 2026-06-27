from pathlib import Path

from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.utils.path_builder import bronze_bhavcopy_path, filename_for, raw_bhavcopy_key
from nse_platform.utils.zip_utils import unzip_single_csv


class EquityBronzeJob(BaseSparkJob):
    def extract(self):
        filename = filename_for(
            self.ec.run_date, settings.UDIFF_CUTOVER_DATE).removesuffix(".zip")
        path = f"{settings.DATA_ROOT}/{raw_bhavcopy_key(self.ec.run_date, filename)}"
        raw_df = self.spark.read.csv(path, header=True)
        return raw_df

    def transform(self, raw):
        self.ec.logger.info("No transformation. skipping...")
        return raw

    def load(self, transformed):
        output_path = f"{settings.DATA_ROOT}/{bronze_bhavcopy_path(self.ec.run_date)}"
        transformed.write.mode("overwrite").parquet(output_path)
        self.ec.logger.info(
            f"Equity bronze data loaded as parquet in {output_path}")
