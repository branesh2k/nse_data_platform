
from nse_platform.config.settings import settings
from nse_platform.jobs.base.base_spark_job import BaseSparkJob
from nse_platform.utils.path_builder import bronze_bhavcopy_path, silver_bhavcopy_path

from pyspark.sql import Window
from pyspark.sql import functions as F


class EquitySilverJob(BaseSparkJob):
    def __init__(self, ec, storage):
        super().__init__(ec, storage)

    def extract(self):
        bronze_path = bronze_bhavcopy_path(self.ec.run_date)
        self.ec.logger.info(f"reading bronze data from {bronze_path}")
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
            df_renamed = df.withColumnsRenamed(COLUMN_MAP_OLD) \
                .select([v for k, v in COLUMN_MAP_OLD.items()])
            df_casted_date = df_renamed.withColumn(
                "trade_date", F.to_date("trade_date", "dd-MMM-yyyy"))

        PRICE_COLS = ["open", "high", "low", "close",
                      "last", "prev_close", "total_traded_value"]
        LONG_COLS = ["total_traded_qty", "total_trades"]
        df_casted = df_casted_date.withColumns({c: F.col(c).cast("double") for c in PRICE_COLS}) \
            .withColumns({c: F.col(c).cast("long") for c in LONG_COLS}).na.drop(subset=["close"])
        w = Window.partitionBy("symbol", "trade_date") \
            .orderBy(F.col("total_traded_value").desc())
        df_cleaned = df_casted.withColumn(
            "rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
        return df_cleaned

    def load(self, transformed):

        output_path = silver_bhavcopy_path(self.ec.run_date)
        transformed.write.mode("overwrite").parquet(output_path)
        self.ec.logger.info(f"silver data loaded as parquet in {output_path}")
