from nse_platform.jobs.base.base_ingest_job import BaseIngestJob
from nse_platform.sources.nse_client import NseClient
from nse_platform.utils.path_builder import filename_for_index, raw_index_key


class NseIndexIngestJob(BaseIngestJob):

    def __init__(self, ec, storage, nse_client):
        super().__init__(ec, storage)
        self.nse_client = nse_client

    def extract(self):
        self.ec.logger.info(f"fetching nse index data for {self.ec.run_date}")
        return self.nse_client.fetch_index(self.ec.run_date)

    def transform(self, raw):
        self.ec.logger.info("No transformation. skipping...")
        return raw

    def load(self, transformed):
        filename = filename_for_index(self.ec.run_date)
        key = raw_index_key(self.ec.run_date, filename)
        if self.storage.exists(key):
            self.ec.logger.info(
                f"index data already ingested at {key}, skipping write")
            return
        self.storage.put(key, transformed)
        self.ec.logger.info(f"wrote {len(transformed)} bytes to {key}")
