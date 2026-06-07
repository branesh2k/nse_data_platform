
from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.utils.path_builder import bronze_index_path, filename_for_index, raw_index_key


class IndexBronzeJob(BaseSparkJob):

    def __init__(self, ec, storage):
        super().__init__(ec, storage)

    def extract(self):
        filename = filename_for_index(self.ec.run_date)
        key = raw_index_key(self.ec.run_date, filename)
        path = f"{settings.LOCAL_DATA_ROOT}/{key}"
        self.ec.logger.info(f"extracting raw data from {path}")
        return self.spark.read.csv(path, header=True)

    def transform(self, raw):
        return raw

    def load(self, transformed):
        output_path = f"{settings.LOCAL_DATA_ROOT}/{bronze_index_path(self.ec.run_date)}"
        transformed.write.mode("overwrite").parquet(output_path)
        self.ec.logger.info(f"Loaded bronze data to {output_path}")
