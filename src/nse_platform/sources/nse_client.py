
import time
import requests
from datetime import date

from nse_platform.utils.dates import format_for_nse_filename
from nse_platform.utils.logging import make_logger


class NseClient:
    """Fetches NSE bhavcopy ZIPs. Handles URL routing, retries, validation."""

    def __init__(self, base_url, udiff_cutover, timeout=30, max_retries=3, logger=None):
        self.logger = logger or make_logger("nse_platform.sources.nse_client")
        self.base_url = base_url.rstrip("/")
        self.udiff_cutover = udiff_cutover
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def filename_for(self, d: date) -> str:
        """Return just the bhavcopy filename (no path) for date d."""

        if d < self.udiff_cutover:
            return f"cm{format_for_nse_filename(d)}bhav.csv.zip"
        else:
            return f"BhavCopy_NSE_CM_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip"

    def build_url(self, d: date) -> str:
        """Pick old or UDiFF URL based on cutover date."""

        filename = self.filename_for(d)
        if d < self.udiff_cutover:
            subdir = f"content/historical/EQUITIES/{d.year}/{d.strftime('%b').upper()}"
        else:
            subdir = "content/cm"
        url = f"{self.base_url}/{subdir}/{filename}"
        self.logger.info(f"resolved URL for {d}: {url}")
        return url

    def fetch_bhavcopy(self, d: date) -> bytes:
        """Download the bhavcopy ZIP for date d. Returns raw bytes."""

        url = self.build_url(d)
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code == 404:
                    raise FileNotFoundError(f"no bhavcopy at {url}")
                resp.raise_for_status()
                if not resp.content.startswith(b"PK"):
                    raise ValueError(
                        f"response is not a ZIP (got {resp.content[:20]!r})"
                    )
                self.logger.info(
                    f"fetched {len(resp.content)} bytes for {d}"
                )
                return resp.content
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(
                        f"giving up after {self.max_retries} attempts: {e}")
                    raise
                sleep_s = 2 ** attempt
                self.logger.warning(
                    f"attempt {attempt + 1} failed ({e}), retrying in {sleep_s}s"
                )
                time.sleep(sleep_s)
