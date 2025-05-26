import logging
import requests

from typing import Optional, Dict, Union, Any, Callable
from prometheus_client import Counter, Histogram
# from dependency_injector import providers, containers
from time import perf_counter
from tenacity import retry, retry_never, retry_if_exception_type, retry_if_result, RetryCallState, Retrying
from tenacity import stop_after_attempt, wait_exponential, before_sleep_log, after_log

logger = logging.getLogger(__name__)

REQUEST_DURATION = Histogram(
    "api_client_request_duration_seconds",
    "Time taken for outbound API requests",
    ["method", "endpoint"]
)

RETRY_COUNTER = Counter(
    "api_client_retries_total",
    "Total number of retry attempts",
    ["method", "endpoint", "reason"]
)

RETRY_DURATION = Histogram(
    "api_client_retry_duration_seconds",
    "Total time spent retrying per endpoint",
    ["method", "endpoint", "reason"]
)

RetryReasonResolver = Callable[[RetryCallState], str]


class RetryPolicy:
    def __init__(self, attempts=3, multiplier=1, min_wait=2, max_wait=10,
                 reason_resolver: Optional[RetryReasonResolver] = None):
        self.attempts = attempts
        self.multiplier = multiplier
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.reason_resolver = reason_resolver or self._default_reason_resolver

    @staticmethod
    def no_retry():
        policy = RetryPolicy(attempts=1)
        policy.build = lambda: Retrying(
            stop=stop_after_attempt(1),
            retry=retry_never,
            reraise=True,
        )
        return policy

    def build(self, method: str = "UNKNOWN", endpoint: str = "UNKNOWN", base_url: str = "",
              body: Optional[Dict] = None,
              trace_id: Optional[str] = None,
              **kwargs) -> Retrying:
        return Retrying(
            stop=stop_after_attempt(self.attempts),
            wait=wait_exponential(multiplier=self.multiplier, min=self.min_wait, max=self.max_wait),
            retry=(retry_if_exception_type(requests.RequestException) |
                   retry_if_result(lambda response: response.status_code >= 500)),
            before_sleep=self._chain_hooks(
                before_sleep_log(logger, logging.DEBUG),
                self._log_retry_attempt_hook(method, endpoint, base_url, body, trace_id, **kwargs),
                self._custom_retry_metrics_hook(method, endpoint),
            ),
            after=after_log(logger, logging.INFO),
            reraise=True,
        )

    @staticmethod
    def _log_retry_attempt_hook(method: str, endpoint: str,
                                base_url: str,
                                body: Optional[Dict] = None,
                                trace_id: Optional[str] = None,
                                **kwargs):
        # trace_id = trace_id or str(uuid.uuid4())

        def hook(retry_state: RetryCallState):
            attempt = retry_state.attempt_number
            exc = retry_state.outcome.exception()
            result = retry_state.outcome.result() if retry_state.outcome else None

            reason = "unknown"
            if exc:
                reason = type(exc).__name__
            elif result is not None:
                reason = f"status_{result.status_code}"

            # Time to sleep until next retry
            sleep_time = retry_state.next_action.sleep if retry_state.next_action else None
            sleep_display = f"{sleep_time:.1f}s" if sleep_time else "?"

            # Elapsed time since first attempt
            elapsed = retry_state.seconds_since_start
            elapsed_display = f"{elapsed:.1f}s"

            logger.warning(
                f"[Retry #{attempt}] [{trace_id}] {method.upper()} {base_url.rstrip('/')}/{endpoint.lstrip('/')}"
                f" — [reason: {reason}]"
                f" — [sleeping for: {sleep_display:>6} (elapsed: {elapsed_display:>6})]"
                f" — body: {body}"
                f" — others: {kwargs}"
            )

        return hook

    @staticmethod
    def _default_reason_resolver(retry_state: RetryCallState) -> str:
        exc = retry_state.outcome.exception()
        result = retry_state.outcome.result() if retry_state.outcome else None

        if exc:
            if isinstance(exc, requests.Timeout):
                return "timeout"
            elif isinstance(exc, requests.ConnectionError):
                return "connection_error"
            return type(exc).__name__
        elif result is not None:
            if result.status_code >= 500:
                return "5xx"
            elif result.status_code == 429:
                return "rate_limit"
            return f"status_{result.status_code}"
        return "unknown"

    def _custom_retry_metrics_hook(self, method: str, endpoint: str):
        def hook(retry_state: RetryCallState):
            reason = self.reason_resolver(retry_state)
            RETRY_COUNTER.labels(method.upper(), endpoint, reason).inc()
            RETRY_DURATION.labels(method.upper(), endpoint, reason).observe(retry_state.idle_for)

        return hook

    @staticmethod
    def _chain_hooks(*hooks):
        def chained_hook(retry_state: RetryCallState):
            for hook in hooks:
                hook(retry_state)

        return chained_hook


def custom_reason_classifier(retry_state: RetryCallState) -> str:
    result = retry_state.outcome.result()
    if result and hasattr(result, "json"):
        try:
            error = result.json().get("error_code", "")
            return f"app_error_{error}"
        except Exception:
            pass
    return RetryPolicy._default_reason_resolver(retry_state)


class ApiClient:
    def __init__(
            self,
            base_url: str,
            headers: Optional[Dict[str, str]] = None,
            timeout: float = 10.0,
            use_persistent_session: bool = False,
            session_verify: bool = True,
            retry_policy: Optional[RetryPolicy] = RetryPolicy()
    ):
        self.base_url = base_url.rstrip("/")
        self.default_headers = headers or {}
        self.timeout = timeout
        self._session: Optional[requests.Session] = requests.Session() if use_persistent_session else None
        self.use_session = use_persistent_session
        self.session_verify = session_verify
        self.retry_policy = retry_policy

    def _get_session(self) -> requests.Session:
        session = self._session or requests.Session()
        session.verify = self.session_verify
        return session

    def close(self):
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _make_request(
            self,
            method: str,
            endpoint: str,
            *,
            headers: Optional[Dict[str, str]] = None,
            json: Optional[Dict] = None,
            data: Optional[Union[str, Dict]] = None,
            params: Optional[Dict[str, str]] = None,
            content_type: Optional[str] = None,
            retry_policy: Optional[RetryPolicy] = None
    ) -> requests.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        merged_headers = {**self.default_headers, **(headers or {})}

        if content_type:
            merged_headers["Content-Type"] = content_type

        session = self._get_session()

        # endpoint_label = endpoint.strip("/").replace("/", "_") or "root"

        def request():
            start_time = perf_counter()
            try:
                return session.request(
                    method=method.upper(),
                    url=url,
                    headers=merged_headers,
                    json=json if content_type == "application/json" else None,
                    data=data if content_type != "application/json" else None,
                    params=params,
                    timeout=self.timeout,
                )
            finally:
                REQUEST_DURATION.labels(method.upper(), endpoint).observe(perf_counter() - start_time)

        try:
            resolved_retry_policy = (self.retry_policy if retry_policy is None else retry_policy)
            retrying: Retrying = resolved_retry_policy.build(method=method,
                                                             endpoint=endpoint,
                                                             base_url=self.base_url,
                                                             body=json if content_type == "application/json" else data)
            response = retrying(request)
            response.raise_for_status()
            return response
        finally:
            if not self.use_session:
                session.close()

    def get(self, endpoint: str, params: Optional[Dict[str, str]] = None, **kwargs) -> Any:
        response = self._make_request("GET", endpoint, params=params, **kwargs)
        return self._handle_response(response)

    def post(
            self,
            endpoint: str,
            *,
            json: Optional[Dict] = None,
            data: Optional[Union[str, Dict]] = None,
            content_type: str = "application/json",
            **kwargs,
    ) -> Any:
        response = self._make_request("POST", endpoint, json=json, data=data, content_type=content_type, **kwargs)
        return self._handle_response(response)

    def put(
            self,
            endpoint: str,
            *,
            json: Optional[Dict] = None,
            data: Optional[Union[str, Dict]] = None,
            content_type: str = "application/json", **kwargs
    ) -> Any:
        response = self._make_request("PUT", endpoint, json=json, data=data, content_type=content_type, **kwargs)
        return self._handle_response(response)

    def delete(
            self,
            endpoint: str,
            *,
            json: Optional[Dict] = None,
            data: Optional[Union[str, Dict]] = None,
            content_type: Optional[str] = None,
            **kwargs,
    ) -> Any:
        response = self._make_request("DELETE", endpoint, json=json, data=data, content_type=content_type, **kwargs)
        return self._handle_response(response)

    def patch(
            self,
            endpoint: str,
            *,
            json: Optional[Dict] = None,
            data: Optional[Union[str, Dict]] = None,
            content_type: str = "application/json",
            **kwargs,
    ) -> Any:
        response = self._make_request("PATCH", endpoint, json=json, data=data, content_type=content_type, **kwargs)
        return self._handle_response(response)

    def health_check(self) -> bool:
        try:
            response = self._make_request("GET", "/health", retry_policy=RetryPolicy.no_retry())
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    @staticmethod
    def _handle_response(response: requests.Response) -> Any:
        if response.status_code == 204:
            return None
        content_type = response.headers.get("Content-Type", "")
        try:
            if "application/json" in content_type:
                return response.json()
            return response.text
        except ValueError:
            return response.text


class MyCustomClient:
    def __init__(self, api_client: ApiClient, owns_client: bool = False):
        self.api_client = api_client
        self.owns_client = owns_client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.owns_client:
            self.api_client.close()

    def get_object_info(self, object_id: str):
        return self.api_client.get(f"/objects/{object_id}")


class MyServiceClient(ApiClient):
    def __init__(self, base_url: str):
        super().__init__(
            base_url=base_url,
            timeout=15,
            retry_policy=RetryPolicy(attempts=5, multiplier=2, min_wait=1, max_wait=20)
        )

    def get_tenant_info(self, tenant_id: str):
        return self.get(f"/tenant/{tenant_id}")

    # @retry(
    #     reraise=True,
    #     stop=stop_after_attempt(5),
    #     wait=wait_exponential(multiplier=1, min=2, max=10),
    #     retry=retry_if_exception_type((requests.RequestException,))
    # )


if __name__ == '__main__':
    # nationalize_api_client = ApiClient(base_url="https://api.nationalize.io/")
    # print(nationalize_api_client.get(endpoint="", params={"name": "russel"}))
    # api_client = ApiClient(base_url="https://24pullrequests.com/")
    # print(api_client.get(endpoint="users.json", params={"page": 2}))
    # restful_api_client = ApiClient(base_url="https://api.restful-api.dev/")
    # print(restful_api_client.get(endpoint="objects"))
    # json = {
    #     "name": "Apple MacBook Pro 1001",
    #     "data": {
    #         "year": 3019,
    #         "price": 1849.99,
    #         "CPU model": "Intel Core i99",
    #         "Hard disk size": "1000 TB"
    #     }
    # }
    # print(restful_api_client.post(endpoint="objects", json=json))

    # print(restful_api_client.get(endpoint="objects", params={"id": "ff80818196f2a23f0196fed3de311e39"}))
    # print(restful_api_client.get(endpoint="objects/ff80818196f2a23f0196fed3de311e39"))
    # print(restful_api_client.patch(endpoint="objects/ff80818196f2a23f0196fed3de311e39", json={'name': 'Apple MacBook BRO 9009'}))
    # print(restful_api_client.put(endpoint="objects/ff80818196f2a23f0196fed3de311e39", json={'name': 'Apple MacBook BRO 9009'}))
    # print(restful_api_client.delete(endpoint="objects/ff80818196f2a23f0196fed3de311e39"))
    # print(restful_api_client.get(endpoint="objects/ff80818196f2a23f0196fed3de311e39"))

    # mcc = MyCustomClient(restful_api_client, True)
    # print(mcc.get_object_info(1))
    #

    wiremock_client = ApiClient(base_url="http://localhost:8080/",
                                retry_policy=RetryPolicy(attempts=5, multiplier=5, min_wait=5, max_wait=60))
    print(wiremock_client.get(endpoint="tenants/123"))
    # print(wiremock_client.get(endpoint="tenants/125"))
    body1 = {"name": "test_tenant"}
    print(wiremock_client.post(endpoint="tenants", json=body1))

# class Container(containers.DeclarativeContainer):
#     config = {}  # providers.Singleton(AppSettings)
#
#     # providers.Resource(...) is like Singleton, but it supports cleanup via .shutdown_resources().
#     api_client_resource = providers.Resource(
#         ApiClient,
#         base_url=config.provided.api_base_url,
#     )
#
#     api_client_singleton = providers.Singleton(ApiClient, base_url="https://api", use_persistent_session=True)
#     api_client_factory = providers.Factory(ApiClient, base_url="https://api", use_persistent_session=False)
#     api_client_thread_local_singleton = providers.ThreadLocalSingleton(ApiClient, base_url="https://api",
#                                                                        use_persistent_session=True)
#
#     my_custom_client = providers.Factory(
#         MyCustomClient,
#         api_client=providers.Factory(
#             ApiClient,
#             base_url=config.provided.myservice_base_url,
#             timeout=15,
#             retry_policy=providers.Callable(RetryPolicy, attempts=5, multiplier=2, min_wait=1, max_wait=20),
#         ),
#         owns_client=True
#     )

# async def main():
#     worker = Worker(
#         task_queue="tenant-queue",
#         workflows=[workflows.TenantWorkflow],
#         activities=[activities.fetch_tenant_data],
#     )
#
#     # Graceful shutdown hook
#     stop_event = asyncio.Event()
#
#     def handle_sigterm():
#         stop_event.set()
#
#     signal.signal(signal.SIGTERM, lambda *_: handle_sigterm())
#     signal.signal(signal.SIGINT, lambda *_: handle_sigterm())
#
#     worker_task = asyncio.create_task(worker.run())
#     await stop_event.wait()
#     worker_task.cancel()
#
#     # 🔥 Cleanup resources
#     await container.shutdown_resources()
#
# asyncio.run(main())
