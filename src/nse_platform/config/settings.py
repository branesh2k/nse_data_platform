import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Settings:
    NSE_BASE_URL: str = os.getenv(
        "NSE_BASE_URL", "https://nsearchives.nseindia.com")
    LOCAL_DATA_ROOT: Path = Path(os.getenv("LOCAL_DATA_ROOT", "./data"))
    ENV: str = os.getenv("ENV", "local")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
