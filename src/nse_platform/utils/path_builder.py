from datetime import date

from nse_platform.utils.dates import format_for_nse_filename


def raw_bhavcopy_key(d: date, filename: str) -> str:
    return (
        f"raw/equity/nse/bhavcopy/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"{filename}"
    )


def bronze_bhavcopy_path(d: date) -> str:
    return (
        f"bronze/equity/nse/bhavcopy/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
    )


def silver_bhavcopy_path(d: date) -> str:
    return (
        f"silver/equity/nse/bhavcopy/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
    )


def filename_for(d: date, cutover: date) -> str:
    """Return just the bhavcopy filename (no path) for date d."""

    if d < cutover:
        return f"cm{format_for_nse_filename(d)}bhav.csv.zip"
    else:
        return f"BhavCopy_NSE_CM_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip"


def filename_for_index(d: date) -> str:
    return f"ind_close_all_{d.strftime('%d%m%Y')}.csv"


def raw_index_key(d: date, filename: str) -> str:
    return (
        f"raw/index/nse/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"{filename}"
    )


def bronze_index_path(d: date) -> str:
    return f"bronze/index/nse/year={d.year}/month={d.month:02d}/day={d.day:02d}/"


def silver_index_path(d: date) -> str:
    return f"silver/index/nse/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
