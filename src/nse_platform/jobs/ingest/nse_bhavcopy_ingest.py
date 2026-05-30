
from nse_platform.jobs.base_ingest_job import BaseIngestJob


class NseBhavcopyIngestJob(BaseIngestJob):

    def __init__(self, ec, storage, nse_client):
        super().__init__(ec, storage)
        self.nse_client = nse_client

    def extract(self):
        self.ec.logger.info(f"fetching nse data for {self.ec.run_date}")
        return self.nse_client.fetch_bhavcopy(self.ec.run_date)

    def transform(self, raw):
        self.ec.logger.info("NO transformation as of now..proceeding to load")
        return raw

    def load(self, transformed):
        key = self._build_key(self.ec.run_date)
        if self.storage.exists(key):
            self.ec.logger.info(f"already ingested at {key}, skipping write")
            return
        self.storage.put(key, transformed)
        self.ec.logger.info(f"wrote {len(transformed)} bytes to {key}")

    def _build_key(self, d):
        """ Builds a partitioned path"""

        filename = self.nse_client.filename_for(d)
        return (
            f"raw/equity/nse/bhavcopy/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"{filename}"
        )
