import logging
import time
import boto3
import botocore
import pybreaker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Optional, Union, Callable, Dict, BinaryIO, Any
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)

# Prometheus metrics
S3_OP_DURATION = Histogram("s3_op_duration_seconds", "Time taken for S3 operations", ["operation", "bucket"])
S3_ERRORS = Counter("s3_errors_total", "S3 error count", ["operation", "bucket", "error"])
S3_CIRCUIT_STATE = Gauge("s3_circuit_state", "0=CLOSED, 1=OPEN, 2=HALF_OPEN", ["bucket"])


class S3CircuitListener(pybreaker.CircuitBreakerListener):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def state_change(self, cb, old_state, new_state):
        state_value = {
            pybreaker.STATE_CLOSED: 0,
            pybreaker.STATE_OPEN: 1,
            pybreaker.STATE_HALF_OPEN: 2,
        }.get(new_state, -1)
        S3_CIRCUIT_STATE.labels(self.bucket).set(state_value)
        logger.warning(f"[Circuit] Bucket {self.bucket} changed from {old_state} → {new_state}")


class S3Client:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str,
        session_token: Optional[str] = None,
    ):
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=session_token,
            region_name=region_name,
        )
        self.client = self.session.client("s3")
        self._breakers: Dict[str, pybreaker.CircuitBreaker] = {}

    def _get_breaker(self, bucket: str) -> pybreaker.CircuitBreaker:
        if bucket not in self._breakers:
            self._breakers[bucket] = pybreaker.CircuitBreaker(
                fail_max=3,
                reset_timeout=300,
                name=f"s3:{bucket}",
                listeners=[S3CircuitListener(bucket)],
            )
        return self._breakers[bucket]

    def _retry(self, fn: Callable):
        return retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(botocore.exceptions.BotoCoreError),
            reraise=True
        )(fn)

    def _observe(self, operation: str, bucket: str, func: Callable):
        breaker = self._get_breaker(bucket)

        @breaker
        def guarded():
            start = time.perf_counter()
            try:
                return self._retry(func)()
            except botocore.exceptions.BotoCoreError as e:
                S3_ERRORS.labels(operation, bucket, type(e).__name__).inc()
                logger.warning(f"[S3 ERROR] {operation} on {bucket}: {type(e).__name__}: {e}")
                raise
            finally:
                duration = time.perf_counter() - start
                S3_OP_DURATION.labels(operation, bucket).observe(duration)

        return guarded()

    def upload_file(self, bucket: str, key: str, data: Union[bytes, BinaryIO], content_type: Optional[str] = None):
        def _upload():
            extra_args = {"ContentType": content_type} if content_type else {}
            if isinstance(data, bytes):
                self.client.put_object(Bucket=bucket, Key=key, Body=data, **extra_args)
            else:
                self.client.upload_fileobj(data, Bucket=bucket, Key=key, ExtraArgs=extra_args)

        logger.info(f"[S3] UPLOAD {bucket}/{key}")
        return self._observe("upload", bucket, _upload)

    def download_file(self, bucket: str, key: str) -> bytes:
        def _download():
            return self.client.get_object(Bucket=bucket, Key=key)["Body"].read()

        logger.info(f"[S3] DOWNLOAD {bucket}/{key}")
        return self._observe("download", bucket, _download)

    def delete_file(self, bucket: str, key: str):
        def _delete():
            self.client.delete_object(Bucket=bucket, Key=key)

        logger.info(f"[S3] DELETE {bucket}/{key}")
        return self._observe("delete", bucket, _delete)

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        def _list():
            paginator = self.client.get_paginator("list_objects_v2")
            return [
                obj["Key"]
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
                for obj in page.get("Contents", [])
            ]

        logger.info(f"[S3] LIST {bucket}/{prefix}")
        return self._observe("list", bucket, _list)

    def file_exists(self, bucket: str, key: str) -> bool:
        def _check():
            self.client.head_object(Bucket=bucket, Key=key)

        try:
            self._observe("exists", bucket, _check)
            return True
        # except botocore.exceptions.ClientError as e:
        except Exception as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def get_bucket_region(self, bucket: str) -> str:
        try:
            response = self.client.get_bucket_location(Bucket=bucket)
            return response.get("LocationConstraint") or "us-east-1"
        except Exception as e:
            logger.warning(f"[S3] Failed to get region for {bucket}: {e}")
            return "us-east-1"


class S3RegionClientFactory:
    def __init__(self, aws_credentials: Dict[str, Any]):
        self.aws_credentials = aws_credentials
        self._clients_by_region: Dict[str, S3Client] = {}
        self._default_client = S3Client(region_name="us-east-1", **aws_credentials)

    def get_client_for_bucket(self, bucket: str) -> S3Client:
        region = self._default_client.get_bucket_region(bucket)

        if region not in self._clients_by_region:
            logger.info(f"[S3] Creating regional client for {region}")
            self._clients_by_region[region] = S3Client(region_name=region, **self.aws_credentials)

        return self._clients_by_region[region]


# from dependency_injector import containers, providers
#
#
# class Container(containers.DeclarativeContainer):
#     config = providers.Configuration()
#
#     s3_factory = providers.Singleton(
#         S3RegionClientFactory,
#         aws_credentials={
#             "aws_access_key_id": config.s3.access_key,
#             "aws_secret_access_key": config.s3.secret_key,
#             "session_token": config.s3.session_token.optional(),
#         }
#     )


# @activity.defn
# async def upload_report_activity(...):
#     s3 = container.s3_factory().get_client_for_bucket("my-tenant-bucket")
#     s3.upload_file("my-tenant-bucket", "report.zip", file_bytes, content_type="application/zip")
