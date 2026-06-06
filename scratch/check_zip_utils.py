from pathlib import Path
from nse_platform.utils.zip_utils import unzip_single_csv

raw = Path(
    "data/raw/equity/nse/bhavcopy/year=2026/month=01/day=01/"
    "BhavCopy_NSE_CM_0_0_0_20260101_F_0000.csv.zip"
).read_bytes()

csv = unzip_single_csv(raw)
text = csv.decode("utf-8")
lines = text.splitlines()
print(f"total bytes: {len(csv)}")
print(f"total lines: {len(lines)}")
print(f"header: {lines[0]}")
print(f"first row: {lines[1]}")
