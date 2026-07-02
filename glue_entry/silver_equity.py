import sys
import os
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "run_date", "data_root", "storage_backend"])
    os.environ["DATA_ROOT"] = args["data_root"]
    os.environ["STORAGE_BACKEND"] = args["storage_backend"]

    from nse_platform.context.execution_context import ExecutionContext
    from nse_platform.storage.factory import make_storage
    from nse_platform.jobs.silver.equity_silver_job import EquitySilverJob

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session

    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    ec = ExecutionContext.for_glue(
        "nse_equity_silver", args["run_date"], spark=spark)
    storage = make_storage()
    EquitySilverJob(ec, storage).run()

    job.commit()


if __name__ == "__main__":
    main()
