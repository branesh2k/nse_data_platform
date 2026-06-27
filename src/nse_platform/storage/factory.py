from nse_platform.config.settings import settings
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.storage.base_storage import BaseStorage
from nse_platform.storage.s3_storage import S3Storage


def make_storage() -> BaseStorage:
    backend = settings.STORAGE_BACKEND
    if backend == "local":
        return LocalStorage(settings.DATA_ROOT)
    if backend == "s3":
        return S3Storage(settings.S3_BUCKET)
    raise ValueError(f"unknown STORAGE_BACKEND: {backend}")
