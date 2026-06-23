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


def check_close_positive(df, logger=None):
    """Validity check: close must be present and positive. Returns (clean_df, bad_df)."""

    is_bad = F.col("close").isNull() | (F.col("close") <= 0)
    dq_reasons = F.array_compact(F.array(
        F.when(is_bad, F.lit("CLOSE_NON_POSITIVE")),))
    df = df.withColumn("dq_reasons", dq_reasons)
    bad_df = df.filter(F.size("dq_reasons") > 0)
    clean_df = df.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    return clean_df, bad_df


def check_ohlc_consistency_equity(df, logger=None):
    """Consistency check: high is the ceiling, low the floor; open/close sit between.
    Any null OHLC value invalidates the bar. Returns (clean_df, bad_df)."""

    ohlc = ["open", "high", "low", "close"]
    any_null = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in ohlc])

    logger.info(f"looking for null in these cols : {ohlc}")

    high_bad = ((F.col("high") < F.col("low"))
                | (F.col("high") < F.col("close"))
                | (F.col("high") < F.col("open"))
                )
    low_bad = ((F.col("open") < F.col("low")) |
               (F.col("close") < F.col("low")))

    dq_reasons = F.array_compact(F.array(
        F.when(any_null, F.lit("NULL_DETECTED")),
        F.when(high_bad, F.lit("HIGH_INCONSISTENCY")),
        F.when(low_bad,  F.lit("LOW_INCONSISTENCY")),
    ),)
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


def check_ohlc_consistency_index(df, logger=None):
    """Consistency check (index): high >= low/open/close, low <= open/close.
    Only fires when OHLC values are present — null OHLC (derived indices like
    arbitrage/futures/dividend-points) is legitimate and passes clean.
    Returns (clean_df, bad_df)."""

    high_bad = ((F.col("high") < F.col("low"))
                | (F.col("high") < F.col("close"))
                | (F.col("high") < F.col("open"))
                )
    low_bad = ((F.col("open") < F.col("low")) |
               (F.col("close") < F.col("low")))

    dq_reasons = F.array_compact(F.array(
        F.when(high_bad, F.lit("HIGH_INCONSISTENCY")),
        F.when(low_bad,  F.lit("LOW_INCONSISTENCY")),
    ),)
    df = df.withColumn("dq_reasons", dq_reasons)
    bad_df = df.filter(F.size("dq_reasons") > 0)
    clean_df = df.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    return clean_df, bad_df


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
