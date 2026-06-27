
from nse_platform.jobs.base.base_ingest_job import BaseIngestJob
from nse_platform.utils.path_builder import raw_bhavcopy_key
from nse_platform.utils.zip_utils import unzip_single_csv


class NseBhavcopyIngestJob(BaseIngestJob):

    def __init__(self, ec, storage, nse_client):
        super().__init__(ec, storage)
        self.nse_client = nse_client

    def extract(self):
        self.ec.logger.info(f"fetching nse data for {self.ec.run_date}")
        return self.nse_client.fetch_bhavcopy(self.ec.run_date)

    def transform(self, raw):
        self.ec.logger.info("No transformation. skipping...")
        return raw

    def load(self, transformed):
        source_name = self.nse_client.filename_for(self.ec.run_date)
        filename = source_name.removesuffix(".zip")
        key = raw_bhavcopy_key(self.ec.run_date, filename)
        if self.storage.exists(key):
            self.ec.logger.info(f"already ingested at {key}, skipping write")
            return
        csv_bytes = unzip_single_csv(transformed)
        self.storage.put(key, csv_bytes)
        self.ec.logger.info(f"wrote {len(csv_bytes)} bytes to {key}")
