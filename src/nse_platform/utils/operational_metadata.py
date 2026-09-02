from datetime import datetime
from pyspark.sql import Row


def log_run(spark, storage_root, job_name, run_date, row_count, status="success"):
    """Appends one row [run_date, row_count, status, timestamp] to a small Delta table"""
    row = Row(
        job_name=job_name,
        run_date=str(run_date),
        row_count=row_count,
        status=status,
        logged_at=str(datetime.now()),
    )
    df = spark.createDataFrame([row])
    path = f"{storage_root}/metadata/operational/job_runs"
    df.write.format("delta").mode("append").save(path)


def log_lineage(spark, storage_root, job_name, run_date, source_paths, target_path):
    """Append one lineage record per run: what sources fed this output."""
    row = Row(
        job_name=job_name,
        run_date=str(run_date),
        source_paths=source_paths,
        target_path=target_path,
        logged_at=str(datetime.now()),
    )
    df = spark.createDataFrame([row])
    path = f"{storage_root}/metadata/lineage/lineage_events"
    df.write.format("delta").mode("append").save(path)
