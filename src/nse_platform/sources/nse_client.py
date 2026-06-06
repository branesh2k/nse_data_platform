
import time
import requests
from datetime import date

from nse_platform.utils.logging import make_logger
from nse_platform.utils.path_builder import filename_for as build_filename


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

    def build_bhavcopy_url(self, d: date) -> str:
        filename = build_filename(d, self.udiff_cutover)
        if d < self.udiff_cutover:
            subdir = f"content/historical/EQUITIES/{d.year}/{d.strftime('%b').upper()}"
        else:
            subdir = "content/cm"
        url = f"{self.base_url}/{subdir}/{filename}"
        self.logger.info(f"resolved bhavcopy URL for {d}: {url}")
        return url

    def build_index_url(self, d: date) -> str:
        filename = f"ind_close_all_{d.strftime('%d%m%Y')}.csv"
        url = f"{self.base_url}/content/indices/{filename}"
        self.logger.info(f"resolved index URL for {d}: {url}")
        return url

    def _get(self, url: str, label: str) -> bytes:
        """Download raw bytes from any NSE URL."""

        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code == 404:
                    raise FileNotFoundError(f"no file at {url}")
                resp.raise_for_status()
                self.logger.info(
                    f"fetched {len(resp.content)} bytes for {label}")
                return resp.content
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(
                        f"giving up after {self.max_retries} attempts: {e}")
                    raise
                sleep_s = 2 ** attempt
                self.logger.warning(
                    f"attempt {attempt + 1} failed ({e}), retrying in {sleep_s}s")
                time.sleep(sleep_s)

    def fetch_bhavcopy(self, d: date) -> bytes:
        url = self.build_bhavcopy_url(d)
        content = self._get(url, label=f"bhavcopy {d}")
        if not content.startswith(b"PK"):
            raise ValueError(f"response is not a ZIP (got {content[:20]!r})")
        return content

    def fetch_index(self, d: date) -> bytes:
        url = self.build_index_url(d)
        content = self._get(url, label=f"index {d}")
        return content

    def filename_for(self, d):
        return build_filename(d, self.udiff_cutover)

    def filename_for_index(self, d):
        return f"ind_close_all_{d.strftime('%d%m%Y')}.csv"
