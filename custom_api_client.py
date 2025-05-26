import logging
import requests
import pybreaker
from pybreaker import CircuitBreaker
from typing import Optional, Dict, Union, Any, Callable
from prometheus_client import Counter
from prometheus_client import Histogram
from tenacity import retry, retry_never, retry_if_exception_type, retry_if_result, RetryCallState, Retrying, RetryError
from tenacity import stop_after_attempt, wait_exponential, before_sleep_log, after_log
import time

from api_client import ApiClient, RetryPolicy

logger = logging.getLogger(__name__)

API_METRICS_COUNTER = Counter(
    "custom_api_entity_requests",
    "Total API metrics per entity per endpoint",
    ["category", "org_id", "request"]
)

API_FAILURE_METRICS_COUNTER = Counter(
    "custom_api_entity_failure_requests",
    "Total API metrics per entity per endpoint",
    ["category", "org_id", "request"]
)

# ENTITY_RETRY_DURATION = Histogram(
#     "custom_api_entity_retry_duration_seconds",
#     "Retry backoff duration per entity",
#     ["category", "org_id", "reason"]
# )

ENTITY_REQUEST_DURATION = Histogram(
    "custom_api_entity_request_duration_seconds",
    "Total request duration per entity",
    ["category", "org_id", "method"]
)

from prometheus_client import Counter, Histogram
from tenacity import RetryError

RETRY_COUNTER = Counter(
    "custom_entity_retries_total",
    "Retry attempts grouped by reason",
    ["category", "org_id", "method", "reason"]
)

ENTITY_RETRY_DURATION = Histogram(
    "custom_api_entity_retry_duration_seconds",
    "Retry time spent grouped by reason",
    ["category", "org_id", "method", "reason"]
)


class CustomApiClient:
    def __init__(self, api_client: ApiClient, owns_client=True):
        """
        :param api_client: Composed reusable ApiClient
        :param account_resolver: Function mapping (category, org_id) → account_id, access_token
        """
        self.api_client = api_client
        # self.account_resolver = account_resolver
        self.breakers = {}
        self.owns_client = owns_client
        self.category = "x"
        self.org_id = "1"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.owns_client:
            self.api_client.close()

    def get_tenant_info(self, object_id: str):
        return self.api_client.get(f"/tenants/{object_id}")

    def _build_headers(self, category: str, org_id: str) -> Dict[str, str]:
        # account = self.account_resolver(category, org_id)
        return {
            # "X-Account-ID": account["account_id"],
            # "Authorization": f"Bearer {account['access_token']}"
        }

    def _log_context(self) -> str:
        return f"[entity: {self.category}:{self.org_id}]"

    # def get_entity_data(self, category: str, org_id: str, path: str) -> Any:
    #     headers = self._build_headers(category, org_id)
    #
    #     # Safe log (no token)
    #     logger.info(f"{self._log_context(category, org_id)} → GET {path}")
    #
    #     try:
    #         return self.api_client.get(
    #             endpoint=path,
    #             headers=headers,
    #             retry_policy=RetryPolicy(
    #                 reason_resolver=self._build_reason_resolver(category, org_id)
    #             )
    #         )
    #     except requests.HTTPError as e:
    #         status = e.response.status_code if e.response else "unknown"
    #         logger.warning(f"{self._log_context(category, org_id)} → failed with {status}")
    #         raise

    def get_entity_data(self, path: str) -> Any:
        # headers = self._build_headers(category, org_id)
        method = "GET"

        logger.warning(f"{self._log_context()} → {method} {path}")
        start_time = time.perf_counter()
        result = None
        try:
            API_METRICS_COUNTER.labels(self.category, self.org_id, path).inc()
            result = self.api_client.get(
                endpoint=path,
                # headers=headers,
                # retry_policy=RetryPolicy(reason_resolver=self._build_reason_resolver(category, org_id))
            )
            duration = time.perf_counter() - start_time
            ENTITY_REQUEST_DURATION.labels(self.category, self.org_id, method).observe(duration)
            return result
        except requests.HTTPError as e:
            status = e.response.status_code if e.response else "unknown"
            logger.warning(f"{self._log_context()} → {method} {path} → failed with {status}")
            API_FAILURE_METRICS_COUNTER.labels(self.category, self.org_id, path).inc()
            raise
        except RetryError as re:
            self.handle_retry_error(
                entity_info={"category": self.category, "org_id": self.org_id},
                method="GET",
                path=path,
                retry_error=re,
                start_time=start_time
            )
            API_FAILURE_METRICS_COUNTER.labels(self.category, self.org_id, path).inc()
            raise

    def handle_retry_error(self, entity_info: dict, method: str, path: str, retry_error: RetryError, start_time: float):
        category = entity_info.get("category", "unknown")
        org_id = entity_info.get("org_id", "unknown")

        last_attempt = retry_error.last_attempt
        duration = time.perf_counter() - start_time

        if last_attempt.failed:
            exc = last_attempt.exception()
            reason = type(exc).__name__.lower()
            logger.error(f"[Retry Failed] {self._log_context()} → {method} {path} → due to exception: {exc}")
        else:
            result = last_attempt.result()
            status = getattr(result, "status_code", "unknown")
            reason = f"status_{status}"
            logger.error(f"[Retry Failed] {self._log_context()} → {method} {path} → due to response: {status}")

        RETRY_COUNTER.labels(category, org_id, method.upper(), reason).inc()
        if duration:
            ENTITY_RETRY_DURATION.labels(category, org_id, method.upper(), reason).observe(duration)

    # def _build_reason_resolver(self, category: str, org_id: str) -> Callable[[RetryCallState], str]:
    #     def resolver(state: RetryCallState) -> str:
    #         reason = RetryPolicy._default_reason_resolver(state)
    #         METRICS_COUNTER.labels(category=category, org_id=org_id, reason=reason).inc()
    #         return reason
    #     return resolver

    # @staticmethod
    # def _build_reason_resolver(category: str, org_id: str):
    #     def resolver(state: RetryCallState) -> str:
    #         reason = RetryPolicy._default_reason_resolver(state)
    #         # Retry metric
    #         METRICS_COUNTER.labels(category, org_id, reason).inc()
    #         if hasattr(state, "idle_for"):
    #             ENTITY_RETRY_DURATION.labels(category, org_id, reason).observe(state.idle_for)
    #
    #         return reason
    #
    #     return resolver

    def _get_breaker(self) -> CircuitBreaker:
        key = (self.category, self.org_id)
        if key not in self.breakers:
            self.breakers[key] = CircuitBreaker(
                fail_max=3,
                reset_timeout=300,
                name=f"{self.category}:{self.org_id}",
                listeners=[LoggingBreakerListener(self.category, self.org_id)],
            )
        return self.breakers[key]

    def _call_with_metrics(
            self,
            method: str,
            endpoint: str,
            call_fn: Callable[[], Any]
    ) -> Any:
        breaker = self._get_breaker()

        @breaker
        def guarded_call():
            start = time.perf_counter()
            try:
                return call_fn()
            finally:
                duration = time.perf_counter() - start
                ENTITY_REQUEST_DURATION.labels(self.category, self.org_id, method).observe(duration)

        try:
            return guarded_call()
        except pybreaker.CircuitBreakerError:
            logger.warning(f"[Circuit OPEN] Blocking call for {self.category}:{self.org_id} to {endpoint}")
            raise


from pybreaker import CircuitBreakerListener, STATE_CLOSED, STATE_OPEN, STATE_HALF_OPEN
from prometheus_client import Gauge

CIRCUIT_STATE = Gauge(
    "custom_api_entity_circuit_state",
    "State of the circuit breaker: 0=CLOSED, 1=OPEN, 2=HALF_OPEN",
    ["category", "org_id"]
)


class LoggingBreakerListener(CircuitBreakerListener):
    def __init__(self, category: str, org_id: str):
        self.entity = f"{category}:{org_id}"

    # def state_change(self, cb, old_state, new_state):
    #     logger.warning(f"[Circuit] {self.entity} changed from {old_state} → {new_state}")

    def state_change(self, cb, old_state, new_state):
        logger.warning(f"[Circuit] {self.entity} changed from {old_state} → {new_state}")

        category, org_id = self.entity.split(":")
        CIRCUIT_STATE.labels(category, org_id).set(
            {
                STATE_CLOSED: 0,
                STATE_OPEN: 1,
                STATE_HALF_OPEN: 2
            }.get(new_state, -1)
        )


from prometheus_client import REGISTRY


def print_metric(metric_name: str):
    for metric in REGISTRY.collect():
        if metric.name == metric_name:
            for sample in metric.samples:
                print(f"{sample.name} {sample.labels} = {sample.value}")


if __name__ == '__main__':
    try:
        wiremock_client = ApiClient(base_url="http://localhost:8080/",
                                    retry_policy=RetryPolicy(attempts=5, multiplier=0.5, min_wait=1, max_wait=60))
        # print(wiremock_client.get(endpoint="tenants/123"))
        custom_api_client = CustomApiClient(wiremock_client)
        print(custom_api_client.get_entity_data("tenants/123"))
        print(custom_api_client.get_entity_data("tenants/123"))
        print(custom_api_client.get_entity_data("tenants/125"))
    except:
        print(custom_api_client.get_entity_data("tenants/125"))
    finally:
        # for metric in REGISTRY.collect():
        #     print(metric)
        print_metric("custom_api_entity_requests")
        print_metric("custom_api_entity_failure_requests")
        print_metric("custom_api_entity_retry_duration_seconds")
        print_metric("custom_api_entity_request_duration_seconds")
