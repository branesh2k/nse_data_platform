

from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.quality.checks import check_relative_return_nullness, check_unique_grain
from nse_platform.utils.partitions import find_prev_silver_date
from nse_platform.utils.path_builder import gold_path, silver_bhavcopy_path, silver_index_path
from pyspark.sql import functions as F


class StockVsMarketGoldJob(BaseSparkJob):

    def extract(self):
        equity_path = f"{settings.LOCAL_DATA_ROOT}/{silver_bhavcopy_path(self.ec.run_date)}"
        self.ec.logger.info(f"Reading Equity silver data from {equity_path}")
        df_equity = self.spark.read.parquet(equity_path)

        index_path = f"{settings.LOCAL_DATA_ROOT}/{silver_index_path(self.ec.run_date)}"
        self.ec.logger.info(
            f"Reading Index silver data from {index_path} and filtering to 'Nifty 50'")
        df_index = self.spark.read.parquet(index_path).filter(
            F.col("index_name") == "Nifty 50")

        df_index_prev = None
        prev_date = find_prev_silver_date(self.ec.run_date, self.storage)
        if prev_date is not None:
            prev_date_path = f"{settings.LOCAL_DATA_ROOT}/{silver_index_path(prev_date)}"
            self.ec.logger.info(
                f"Reading last run Index data from {prev_date_path} and filtering to 'Nifty 50")
            df_index_prev = self.spark.read.parquet(
                prev_date_path).filter(F.col("index_name") == "Nifty 50")

        return (df_equity, df_index, df_index_prev)

    def transform(self, raw):
        df_equity, df_index, df_index_prev = raw

        df_equity = df_equity.withColumn("stock_return", (F.col(
            "close")-F.col("prev_close"))/F.col("prev_close"))

        if df_index_prev is None:
            df_index = df_index.withColumn(
                "prev_close", F.lit(None).cast("double"))
        else:
            df_index = df_index.join(df_index_prev, on="index_name") \
                .select(df_index["*"], df_index_prev["close"].alias("prev_close"))

        df_index = df_index.withColumn("index_return", (F.col(
            "close")-F.col("prev_close"))/F.col("prev_close"))

        df_equity = (
            df_equity.withColumnRenamed("open", "stock_open")
            .withColumnRenamed("high", "stock_high")
            .withColumnRenamed("low", "stock_low")
            .withColumnRenamed("close", "stock_close")
            .withColumnRenamed("last", "stock_last")
            .withColumnRenamed("prev_close", "stock_prev_close")
        )

        df_index = (
            df_index.withColumnRenamed("open", "index_open")
            .withColumnRenamed("high", "index_high")
            .withColumnRenamed("low", "index_low")
            .withColumnRenamed("close", "index_close")
            .withColumnRenamed("prev_close", "index_prev_close")
        )

        df_gold = df_equity.join(df_index, on="trade_date", how="left")
        df_gold = df_gold.withColumn("relative_return", F.col(
            "stock_return")-F.col("index_return"))

        # # ---------------------------------------------------------------------
        # # TEMP TEST
        # df_gold = df_gold.withColumn(
        #     "relative_return",
        #     F.when(F.col("stock_return") > 0.05, F.lit(None).cast(
        #         "double"))   # arbitrary deterministic slice
        #     .otherwise(F.col("relative_return"))
        # )
        return df_gold

    def validate(self, transformed_data):
        check_unique_grain(transformed_data, keys=[
                           "symbol", "trade_date"], logger=self.ec.logger)
        check_relative_return_nullness(transformed_data, logger=self.ec.logger)
        return transformed_data

    def load(self, transformed):
        path = f"{settings.LOCAL_DATA_ROOT}/{gold_path(self.ec.run_date)}"
        self.ec.logger.info(f"Writing gold data to {path}")
        self.ec.logger.info("-----PIPELINE COMPLETED-----")
        transformed.write.format("delta").mode("overwrite").save(path)
