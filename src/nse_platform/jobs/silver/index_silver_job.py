from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.quality.checks import check_required_columns, load_contract, run_contract_checks
from nse_platform.utils.path_builder import bronze_index_path, quarantine_silver_path, silver_index_path

from pyspark.sql import functions as F
from pyspark.sql import Window


class IndexSilverJob(BaseSparkJob):

    def extract(self):
        path = f"{settings.DATA_ROOT}/{bronze_index_path(self.ec.run_date)}"
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
            .withColumn("volume", F.expr("try_cast(volume as long)"))
        double_cols = ["open", "high", "low", "close",
                       "turnover_in_rs_cr", "pe", "pb", "div_yield"]
        df = df.withColumns({c: F.expr(f"try_cast({c} as double)")
                            for c in double_cols})

        w = Window.partitionBy("index_name", "trade_date") \
            .orderBy(F.col("close").desc())
        df = df.withColumn("rn", F.row_number().over(w)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")

        return df

    def validate(self, transformed_data):
        contract = load_contract(
            f"{settings.CONTRACTS_ROOT}/silver_index.yaml")
        clean, quarantine = run_contract_checks(
            transformed_data, contract, run_date=self.ec.run_date, logger=self.ec.logger
        )

        if quarantine is not None:
            bad_count = quarantine.count()
            if bad_count > 0:
                self.ec.logger.info("Bad data found!!")
                qpath = f"{settings.DATA_ROOT}/{quarantine_silver_path(self.ec.run_date)}"
                quarantine.write.mode("overwrite").parquet(qpath)
                self.ec.logger.warning(
                    f"[dq] quarantined {bad_count} rows → {qpath}")

        return clean

    def load(self, transformed):
        output_path = f"{settings.DATA_ROOT}/{silver_index_path(self.ec.run_date)}"
        transformed.write.mode("overwrite").parquet(output_path)
        self.ec.logger.info(
            f"Index silver data loaded as parquet in {output_path}")
