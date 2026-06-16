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
