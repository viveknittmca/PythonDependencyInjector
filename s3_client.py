import logging
import time
from pathlib import Path

import boto3
import botocore
import pybreaker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Optional, Union, Callable, Dict, BinaryIO, Any
from prometheus_client import Counter, Histogram, Gauge
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
S3_SUCCESS_OP_DURATION = Histogram("s3_success_op_duration_seconds",
                                   "Time taken for Successful S3 operations",
                                   ["operation", "bucket"])
S3_FAILURE_OP_DURATION = Histogram("s3_failure_op_duration_seconds",
                                   "Time taken for Failed S3 operations",
                                   ["operation", "bucket"])
S3_ERRORS = Counter("s3_errors_total",
                    "S3 error count",
                    ["operation", "bucket", "error"])
S3_CIRCUIT_STATE = Gauge("s3_circuit_state",
                         "0=CLOSED, 1=HALF_OPEN, 2=OPEN",
                         ["bucket"])


class S3CircuitListener(pybreaker.CircuitBreakerListener):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def state_change(self, cb, old_state, new_state):
        state_to_int = {'closed': 0, 'half_open': 1, 'open': 2}
        S3_CIRCUIT_STATE.labels(self.bucket).set(state_to_int.get(new_state.name, -1))
        logger.warning(f"[Circuit] Bucket {self.bucket} changed from {old_state.name} → {new_state.name}")


class S3Client:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        session_token: Optional[str] = None,
        region_name: Optional[str] = None,
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

    def is_retryable_exception(e: BaseException) -> bool:
        if isinstance(e, ClientError):
            return e.response["Error"]["Code"] != "404"
        return isinstance(e, BotoCoreError)

    @staticmethod
    def _retry(fn: Callable):
        return retry(
            stop=stop_after_attempt(2),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((BotoCoreError, ClientError)),
            reraise=True
        )(fn)

    def _observe(self, operation: str, bucket: str, func: Callable):
        breaker = self._get_breaker(bucket)

        @breaker
        def guarded():
            start = time.perf_counter()
            try:
                result = self._retry(func)()
                duration = time.perf_counter() - start
                S3_SUCCESS_OP_DURATION.labels(operation, bucket).observe(duration)
                return result
            except Exception as e:
                duration = time.perf_counter() - start
                S3_FAILURE_OP_DURATION.labels(operation, bucket).observe(duration)
                S3_ERRORS.labels(operation, bucket, type(e).__name__).inc()
                logger.warning(f"[S3 ERROR] {operation} on {bucket}: {type(e).__name__}: {e}")
                raise
        return guarded()

    def upload_file(
            self,
            bucket: str,
            key: str,
            data: Union[str, Path, bytes, BinaryIO],
            content_type: Optional[str] = None
    ) -> bool:
        def _upload():
            extra_args = {"ContentType": content_type} if content_type else {}

            try:
                # Upload from in-memory bytes
                if isinstance(data, bytes):
                    self.client.put_object(Bucket=bucket, Key=key, Body=data, **extra_args)

                # Upload from file path (str or Path)
                elif isinstance(data, (str, Path)):
                    path = Path(data)
                    if not path.is_file():
                        logger.error(f"[S3 ERROR] File not found: {path}")
                        raise FileNotFoundError(f"File not found: {path}")
                    self.client.upload_file(Filename=str(path), Bucket=bucket, Key=key, ExtraArgs=extra_args)

                # Upload from file-like object
                elif hasattr(data, "read") and callable(data.read):
                    self.client.upload_fileobj(data, Bucket=bucket, Key=key, ExtraArgs=extra_args)

                else:
                    raise TypeError(f"Unsupported input type: {type(data).__name__}")

                logger.info(f"[S3] UPLOADED {bucket}/{key}")
                return True

            except botocore.exceptions.BotoCoreError as e:
                logger.error(f"[S3 ERROR] Failed to upload {bucket}/{key}: {e}")
                raise

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
        except botocore.exceptions.ClientError as e:
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


from prometheus_client import REGISTRY


def print_metric(metric_name: str):
    for metric in REGISTRY.collect():
        if metric.name == metric_name:
            for sample in metric.samples:
                print(f"{sample.name} {sample.labels} = {sample.value}")


if __name__ == '__main__':
    try:
        s3_client = S3Client("","")
        # print(s3_client.file_exists('vivekrajendran', 'test.txt'))
        print(s3_client.delete_file('vivekrajendran', 'test/1/requirements.txt'))

        # print(s3_client.upload_file('vivekrajendran', 'test/1/requirements.txt', Path('/Users/vivek/Github'
        #                                                                               '/PythonDependencyInjector'
        #                                                                               '/requirements1.txt')))
        # for i in range(5):
        #     try:
        #         s3_client.download_file(bucket="vivekrajendran", key="test.txt")
        #     except Exception as e:
        #         print(f"Attempt {i + 1}: {type(e).__name__} - {e}")
        # with open("downloaded_requirements.txt", "wb") as w:
        #     w.write(s3_client.download_file('vivekrajendran', 'test/1/requirements1.txt'))
    finally:
        # for metric in REGISTRY.collect():
        #     print(metric)
        print_metric("s3_success_op_duration_seconds")
        print_metric("s3_failure_op_duration_seconds")
        print_metric("s3_errors")
        print_metric("s3_circuit_state")


# class S3RegionClientFactory:
#     def __init__(self, aws_credentials: Dict[str, Any]):
#         self.aws_credentials = aws_credentials
#         self._clients_by_region: Dict[str, S3Client] = {}
#         self._default_client = S3Client(region_name="us-east-1", **aws_credentials)
#
#     def get_client_for_bucket(self, bucket: str) -> S3Client:
#         region = self._default_client.get_bucket_region(bucket)
#
#         if region not in self._clients_by_region:
#             logger.info(f"[S3] Creating regional client for {region}")
#             self._clients_by_region[region] = S3Client(region_name=region, **self.aws_credentials)
#
#         return self._clients_by_region[region]


# if __name__ == '__main__':
#     aws_creds = {
#             "aws_access_key_id": ,
#             "aws_secret_access_key": ,
#             "session_token":
#     }
#     S3RegionClientFactory(aws_creds)

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
