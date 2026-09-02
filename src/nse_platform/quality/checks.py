import yaml
from functools import reduce

from pyspark.sql import functions as F


def check_required_columns(df, expected: list[str], logger=None):
    """[schema] df must contain all expected source columns."""
    actual = set(df.columns)
    missing = [c for c in expected if c not in actual]
    if missing:
        raise ValueError(
            f"[schema] source missing expected columns: {missing} "
            f"(got: {sorted(actual)})"
        )
    if logger:
        logger.info(f"[schema] all {len(expected)} expected columns present")
    return df


def check_positive(df, column: str, reason_code: str, logger=None):
    """Validity check: `column` must be present and positive. Returns (clean_df, bad_df)."""
    is_bad = F.col(column).isNull() | (F.col(column) <= 0)
    dq_reasons = F.array_compact(F.array(
        F.when(is_bad, F.lit(reason_code)),))
    df = df.withColumn("dq_reasons", dq_reasons)
    bad_df = df.filter(F.size("dq_reasons") > 0)
    clean_df = df.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    return clean_df, bad_df


def check_ohlc_consistency(df, treat_null_as_bad: bool = True, logger=None):
    """Consistency check: high is the ceiling, low the floor; open/close sit between.
    Returns (clean_df, bad_df).

    - treat_null_as_bad = True  → (any null in open/high/low/close invalidates the bar)
    - treat_null_as_bad = False → (null OHLC is legitimate for derived indices — e.g. arbitrage,
                                    futures, dividend-points — and passes clean)"""

    ohlc = ["open", "high", "low", "close"]

    high_bad = ((F.col("high") < F.col("low"))
                | (F.col("high") < F.col("close"))
                | (F.col("high") < F.col("open")))
    low_bad = ((F.col("open") < F.col("low")) |
               (F.col("close") < F.col("low")))

    reasons = [
        F.when(high_bad, F.lit("HIGH_INCONSISTENCY")),
        F.when(low_bad, F.lit("LOW_INCONSISTENCY")),
    ]
    if treat_null_as_bad:
        if logger:
            logger.info(f"looking for null in these cols : {ohlc}")
        any_null = reduce(lambda a, b: a | b, [
                          F.col(c).isNull() for c in ohlc])
        reasons.insert(0, F.when(any_null, F.lit("NULL_DETECTED")))

    dq_reasons = F.array_compact(F.array(*reasons))
    df = df.withColumn("dq_reasons", dq_reasons)
    bad_df = df.filter(F.size("dq_reasons") > 0)
    clean_df = df.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    return clean_df, bad_df


def check_unique_grain(df, keys: list[str], logger=None):
    """[uniqueness] Post-condition assertion: df must have one row per grain.
    A duplicate here means the transform's dedup failed — a pipeline bug. Fail loud."""

    dupes = df.groupBy(*keys).count().filter(F.col("count") > 1)
    dup_count = dupes.count()

    if dup_count > 0:
        examples = [tuple(r[k] for k in keys)
                    for r in dupes.limit(5).collect()]
        raise ValueError(
            f"[uniqueness] dedup post-condition violated: {dup_count} duplicate "
            f"grain(s) on {keys}. Examples: {examples}")
    if logger:
        logger.info(f"[uniqueness] grain unique on {keys} — assertion held")


def check_timeliness(df, expected_date, logger=None):
    """[timeliness] Gate: every row's trade_date must equal the run date.
    A wrong date means a stale or mis-fetched file — the whole run is wrong. Fail loud."""

    wrong = df.filter(F.col("trade_date") != F.lit(expected_date))
    wrong_count = wrong.count()

    if wrong_count > 0:
        bad_dates = [r["trade_date"] for r in wrong.select(
            "trade_date").distinct().limit(5).collect()]
        raise ValueError(
            f"[timeliness] {wrong_count} rows with trade_date != {expected_date}. "
            f"Unexpected dates: {bad_dates}"
        )

    if logger:
        logger.info(
            f"[timeliness] all rows dated {expected_date} — assertion held")


def check_relative_return_nullness(df, logger=None):
    """valid states: all null (None-branch) OR none null (normal run).
    a partial mix means the index join matched some stocks but not others — a bug."""
    total = df.count()
    null_rr = df.filter(F.col("relative_return").isNull()).count()

    if null_rr != 0 and null_rr != total:
        raise ValueError(
            f"[gold] relative_return null for {null_rr}/{total} rows — "
            f"expected all-null (first day) or none-null (normal). "
            f"Partial nulls indicate an inconsistent index join."
        )
    if logger:
        logger.info(
            f"[gold] relative_return consistency held ({null_rr}/{total} null)")

# ---------------------


def load_contract(path: str) -> dict:
    """Read a contract YAML file into a plain dict.
    Supports both local paths and s3:// paths"""

    if path.startswith("s3://"):
        import boto3
        bucket, key = path.replace("s3://", "", 1).split("/", 1)
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        text = obj["Body"].read().decode("utf-8")
        return yaml.safe_load(text)

    with open(path) as f:
        return yaml.safe_load(f)


def run_contract_checks(df, contract: dict, run_date=None, logger=None):
    grain = contract.get("grain", [])
    quarantine_parts = []
    clean = df

    for check in contract.get("checks", []):
        ctype = check["type"]

        if ctype == "timeliness":
            check_timeliness(clean, run_date, logger=logger)

        elif ctype == "positive":
            column = check["column"]
            reason_code = check["reason_code"]
            clean, bad = check_positive(
                clean, column, reason_code, logger=logger)
            quarantine_parts.append(bad)

        elif ctype == "ohlc_consistency":
            treat_null_as_bad = check.get("treat_null_as_bad", True)
            clean, bad = check_ohlc_consistency(
                clean, treat_null_as_bad=treat_null_as_bad, logger=logger)
            quarantine_parts.append(bad)

        elif ctype == "unique_grain":
            check_unique_grain(clean, keys=grain, logger=logger)

        else:
            raise ValueError(f"unknown contract check type: '{ctype}'")

    quarantine = None
    if quarantine_parts:
        quarantine = quarantine_parts[0]
        for part in quarantine_parts[1:]:
            quarantine = quarantine.unionByName(part)

    return clean, quarantine
