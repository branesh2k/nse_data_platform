from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.quality.checks import check_close_positive, check_ohlc_consistency_index, check_required_columns, check_timeliness, check_unique_grain
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

        # ---------------------------------------------------
        # # TEMP TEST
        # self.ec.logger.debug("Manually injecting fake data for validation")
        # df = df.withColumn(
        #     "close",
        #     F.when(F.col("index_name") == "Nifty 50",
        #            F.lit(-1.0))   # close_positive → bad
        #     .otherwise(F.col("close"))
        # ).withColumn(
        #     "high",
        #     F.when(F.col("index_name") == "Nifty Bank",
        #            F.lit(1.0))  # high < low → OHLC bad
        #     .otherwise(F.col("high"))
        # )

        return df

    def validate(self, transformed_data):
        check_timeliness(transformed_data, self.ec.run_date,
                         logger=self.ec.logger)
        close_clean, close_bad = check_close_positive(
            transformed_data, logger=self.ec.logger)
        ohlc_clean, ohlc_bad = check_ohlc_consistency_index(
            close_clean, logger=self.ec.logger)
        check_unique_grain(ohlc_clean, keys=[
                           "index_name", "trade_date"], logger=self.ec.logger)

        quarantine = close_bad.unionByName(ohlc_bad)
        bad_count = quarantine.count()
        if bad_count > 0:
            self.ec.logger.info("Bad data found!!")
            qpath = f"{settings.DATA_ROOT}/{quarantine_silver_path(self.ec.run_date)}"
            quarantine.write.mode("overwrite").parquet(qpath)
            self.ec.logger.warning(
                f"[dq] quarantined {bad_count} rows → {qpath}")

        return ohlc_clean

    def load(self, transformed):
        output_path = f"{settings.DATA_ROOT}/{silver_index_path(self.ec.run_date)}"
        transformed.write.mode("overwrite").parquet(output_path)
        self.ec.logger.info(
            f"Index silver data loaded as parquet in {output_path}")
