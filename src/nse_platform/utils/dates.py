from datetime import date, timedelta

from nse_platform.utils.path_builder import silver_index_path

NSE_HOLIDAYS: set[date] = {
    date(2024, 1, 26),   # Republic Day
    date(2024, 3, 8),    # Mahashivratri
    date(2025, 1, 26),   # Republic Day
}


def is_trading_day(d: date) -> bool:
    """True if d is a weekday and not in NSE_HOLIDAYS."""
    if d.weekday() < 5 and d not in NSE_HOLIDAYS:
        return True
    else:
        return False


def format_for_nse_filename(d: date) -> str:
    """Return d formatted as 'DDMMMYYYY' for NSE bhavcopy filenames."""
    return d.strftime("%d%b%Y").upper()


def parse_run_date(s: str) -> date:
    """Parse an ISO date string ('YYYY-MM-DD') into a date object."""
    return date.fromisoformat(s)


def find_prev_silver_date(run_date: date, storage, max_lookback=10):
    for i in range(max_lookback):
        candidate = run_date - timedelta(days=1 + i)
        if storage.exists(silver_index_path(candidate)):
            return candidate
    return None
