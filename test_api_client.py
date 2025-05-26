import json

import respx
import pytest
import httpx
from api_client import ApiClient, RetryPolicy


@pytest.fixture
def client():
    # return ApiClient(base_url="https://mock.local", retry_policy=RetryPolicy(attempts=1))
    return ApiClient(base_url="https://api.restful-api.dev/", retry_policy=RetryPolicy(attempts=2))


@respx.mock
def test_get_success(client):
    j = json.dumps({'id': '1', 'name': 'Google Pixel 6 Pro', 'data': {'color': 'Cloudy White', 'capacity': '128 GB'}})
    route = respx.get("https://api.restful-api.dev/objects/1").mock(
        return_value=httpx.Response(200, json=j)
    )
    result = client.get("/objects/1")
    assert json.dumps(result) == j
    assert route.called


@respx.mock
def test_post_with_payload(client):
    route = respx.post("https://mock.local/create").mock(
        return_value=httpx.Response(201, json={"id": 1})
    )
    result = client.post("/create", json={"name": "Item"})
    assert result["id"] == 1
    assert route.called


@respx.mock
def test_delete_no_content(client):
    route = respx.delete("https://mock.local/resource/123").mock(
        return_value=httpx.Response(204)
    )
    result = client.delete("/resource/123")
    assert result is None  # for 204
    assert route.called


# import unittest
#
#
# class MyTestCase(unittest.TestCase):
#     def test_something(self):
#         self.assertEqual(True, False)  # add assertion here
#
#
# if __name__ == '__main__':
#     unittest.main()

# test_get_200_json()
#
# test_post_503_then_200_with_retry_metrics()
#
# test_204_no_content_is_none()
#
# test_timeout_is_logged_with_reason()
#
# test_health_check_returns_false_on_fail()
#
