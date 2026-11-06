from pathlib import Path

from nse_platform.storage.base_storage import BaseStorage


class LocalStorage(BaseStorage):
    def __init__(self, root: str | Path):
        self.root = Path(root)

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

    def prefix_exists(self, prefix: str) -> bool:
        full_path = self.root / prefix
        return full_path.is_dir() and any(full_path.iterdir())
