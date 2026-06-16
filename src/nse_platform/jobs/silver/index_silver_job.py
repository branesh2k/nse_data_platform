from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.quality.checks import check_required_columns
from nse_platform.utils.path_builder import bronze_index_path, silver_index_path

from pyspark.sql import functions as F
from pyspark.sql import Window


class IndexSilverJob(BaseSparkJob):

    def extract(self):
        path = f"{settings.LOCAL_DATA_ROOT}/{bronze_index_path(self.ec.run_date)}"
        return self.spark.read.parquet(path)

    def transform(self, raw):
        map_columns = {
            "Index Name": "index_name",
            "Index Date": "trade_date",
            "Open Index Value": "open",
            "High Index Value": "high",
            "Low Index Value": "low",
            "Closing Index Value": "close",
            "Volume": "volume",
            "Turnover (Rs. Cr.)": "turnover_in_rs_cr",
            "P/E": "pe",
            "P/B": "pb",
            "Div Yield": "div_yield",
        }
        check_required_columns(raw, list(map_columns.keys()), self.ec.logger)
        df = raw.withColumnsRenamed(map_columns).select(
            [c for c in map_columns.values()]
        )

        df = df.withColumn("trade_date", F.to_date("trade_date", "dd-MM-yyyy")) \
            .withColumn("volume", F.col("volume").try_cast("long"))
        double_cols = ["open", "high", "low", "close",
                       "turnover_in_rs_cr", "pe", "pb", "div_yield"]
        df = df.withColumns({c: F.col(c).try_cast("double")
                            for c in double_cols})
        df = df.na.drop(subset=["close"])

        w = Window.partitionBy("index_name", "trade_date") \
            .orderBy(F.col("close").desc())
        df = df.withColumn("rn", F.row_number().over(w)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")

        return df

    def load(self, transformed):
        output_path = f"{settings.LOCAL_DATA_ROOT}/{silver_index_path(self.ec.run_date)}"
        transformed.write.mode("overwrite").parquet(output_path)
        self.ec.logger.info(
            f"Equity silver data loaded as parquet in {output_path}")
