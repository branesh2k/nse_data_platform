import boto3
from botocore.exceptions import ClientError

from nse_platform.storage.base_storage import BaseStorage


class S3Storage(BaseStorage):
    def __init__(self, bucket: str, client=None):
        self.bucket = bucket
        self._s3 = client or boto3.client("s3")

    def put(self, key: str, data: bytes) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def get(self, key: str) -> bytes:
        try:
            response = self._s3.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"s3://{self.bucket}/{key}") from e
            raise

    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise
