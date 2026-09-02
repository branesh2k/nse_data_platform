import os
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

load_dotenv()


class Settings:
    NSE_BASE_URL: str = os.getenv(
        "NSE_BASE_URL", "https://nsearchives.nseindia.com")

    STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "local")

    DATA_ROOT: str = os.getenv("DATA_ROOT", "./data")
    CONTRACTS_ROOT: str = os.getenv("CONTRACTS_ROOT", "./contracts")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "nse-platform")

    ENV: str = os.getenv("ENV", "local")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    UDIFF_CUTOVER_DATE: date = date.fromisoformat(
        os.getenv("UDIFF_CUTOVER_DATE", "2024-07-08"))


settings = Settings()
