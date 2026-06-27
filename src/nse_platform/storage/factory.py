from nse_platform.config.settings import settings
from nse_platform.storage.local_storage import LocalStorage
from nse_platform.storage.base_storage import BaseStorage


def make_storage() -> BaseStorage:
    backend = settings.STORAGE_BACKEND
    if backend == "local":
        return LocalStorage(settings.DATA_ROOT)
    raise ValueError(f"unknown STORAGE_BACKEND: {backend}")
