
from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.quality.checks import check_close_positive, check_ohlc_consistency_equity, check_required_columns, check_timeliness, check_unique_grain
from nse_platform.utils.path_builder import bronze_bhavcopy_path, quarantine_equity_path, silver_bhavcopy_path

from pyspark.sql import Window
from pyspark.sql import functions as F


class EquitySilverJob(BaseSparkJob):
    def __init__(self, ec, storage):
        super().__init__(ec, storage)

    def extract(self):
        path = bronze_bhavcopy_path(self.ec.run_date)
        bronze_path = f"{settings.DATA_ROOT}/{path}"
        self.ec.logger.info(f"Reading Equity bronze data from {bronze_path}")
        return self.spark.read.parquet(bronze_path)

    def transform(self, raw):
        if self.ec.run_date >= settings.UDIFF_CUTOVER_DATE:
            df = raw.filter(F.col("SctySrs").isin("BE", "EQ"))
            COLUMN_MAP_NEW = {
                "TckrSymb": "symbol",
                "SctySrs": "series",
                "TradDt": "trade_date",
                "OpnPric": "open",
                "HghPric": "high",
                "LwPric": "low",
                "ClsPric": "close",
                "LastPric": "last",
                "PrvsClsgPric": "prev_close",
                "TtlTradgVol": "total_traded_qty",
                "TtlTrfVal": "total_traded_value",
                "TtlNbOfTxsExctd": "total_trades",
                "ISIN": "isin",
            }
            check_required_columns(
                raw, list(COLUMN_MAP_NEW.keys()), self.ec.logger)
            df_renamed = df.withColumnsRenamed(COLUMN_MAP_NEW) \
                .select([v for k, v in COLUMN_MAP_NEW.items()])
            df_casted_date = df_renamed.withColumn(
                "trade_date", F.col("trade_date").cast("date"))

        else:
            df = raw.filter(F.col("SERIES").isin("BE", "EQ"))
            COLUMN_MAP_OLD = {
                "SYMBOL": "symbol",
                "SERIES": "series",
                "TIMESTAMP": "trade_date",
                "OPEN": "open",
                "HIGH": "high",
                "LOW": "low",
                "CLOSE": "close",
                "LAST": "last",
                "PREVCLOSE": "prev_close",
                "TOTTRDQTY": "total_traded_qty",
                "TOTTRDVAL": "total_traded_value",
                "TOTALTRADES": "total_trades",
                "ISIN": "isin",
            }
            check_required_columns(
                raw, list(COLUMN_MAP_OLD.keys()), self.ec.logger)
            df_renamed = df.withColumnsRenamed(COLUMN_MAP_OLD) \
                .select([v for k, v in COLUMN_MAP_OLD.items()])
            df_casted_date = df_renamed.withColumn(
                "trade_date", F.to_date("trade_date", "dd-MMM-yyyy"))

        PRICE_COLS = ["open", "high", "low", "close",
                      "last", "prev_close", "total_traded_value"]
        LONG_COLS = ["total_traded_qty", "total_trades"]
        df_casted = df_casted_date.withColumns({c: F.col(c).cast("double") for c in PRICE_COLS}) \
            .withColumns({c: F.col(c).cast("long") for c in LONG_COLS})
        w = Window.partitionBy("symbol", "trade_date") \
            .orderBy(F.col("total_traded_value").desc())
        df_cleaned = df_casted.withColumn(
            "rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
# # --------------------------------------------------------------------------------------------------
        # # Temp test
        # self.ec.logger.debug("Manually injecting fake data for validation")
        # df_cleaned = df_cleaned.withColumn(
        #     "close",
        #     F.when(F.col("symbol").isin("ALKYLAMINE",
        #            "ASMS", "CUBEXTUB"), F.lit(-1.0))
        #     .otherwise(F.col("close"))
        # )

        return df_cleaned

    def validate(self, transformed_data):

        check_timeliness(transformed_data, self.ec.run_date,
                         logger=self.ec.logger)

        close_clean, close_bad = check_close_positive(
            transformed_data, logger=self.ec.logger)
        ohlc_clean, ohlc_bad = check_ohlc_consistency_equity(
            close_clean, logger=self.ec.logger)

        check_unique_grain(ohlc_clean, keys=[
                           "symbol", "trade_date"], logger=self.ec.logger)

        quarantine = close_bad.unionByName(ohlc_bad)
        bad_count = quarantine.count()
        if bad_count > 0:
            self.ec.logger.info("Bad data found!!")
            qpath = f"{settings.DATA_ROOT}/{quarantine_equity_path(self.ec.run_date)}"
            quarantine.write.mode("overwrite").parquet(qpath)
            self.ec.logger.warning(
                f"[dq] quarantined {bad_count} rows → {qpath}")
        return ohlc_clean

    def load(self, transformed):

        output_path = f"{settings.DATA_ROOT}/{silver_bhavcopy_path(self.ec.run_date)}"
        transformed.write.mode("overwrite").parquet(output_path)
        self.ec.logger.info(
            f"Equity silver data loaded as parquet in {output_path}")
