from pathlib import Path

from nse_platform.storage.base_storage import BaseStorage


class LocalStorage(BaseStorage):
    def __init__(self, root: Path):
        self.root = root

    def put(self, key: str, data: bytes) -> None:
        full_path = self.root / key
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)

    def get(self, key: str) -> bytes:
        full_path = self.root / key
        return full_path.read_bytes()

    def exists(self, key: str) -> bool:
        full_path = self.root / key
        return full_path.exists()
