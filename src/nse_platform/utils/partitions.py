
from datetime import date, timedelta

from nse_platform.utils.path_builder import silver_index_path


def find_prev_silver_date(run_date: date, storage, max_lookback=10):
    for i in range(max_lookback):
        candidate = run_date - timedelta(days=1 + i)
        if storage.exists(silver_index_path(candidate)):
            return candidate
    return None
