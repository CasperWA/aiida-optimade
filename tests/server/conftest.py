# pylint: disable=redefined-outer-name
from typing import List

import pytest


@pytest.fixture(scope="module")
def client():
    """Return TestClient for OPTIMADE server"""
    from .utils import client_factory

    return client_factory()()


@pytest.fixture
def get_good_response(client):
    """Get OPTIMADE response with some sanity checks"""

    def _get_good_response(request):
        try:
            response = client.get(request)
            assert response.status_code == 200, f"Request failed: {response.json()}"
            response = response.json()
        except Exception as exc:
            print("Request attempted:")
            print(f"{client.base_url}{client.version}{request}")
            raise exc
        else:
            return response

    return _get_good_response


@pytest.fixture
def check_response(get_good_response):
    """Fixture to check response using client fixture"""
    from optimade.server.config import CONFIG

    def _check_response(
        request: str,
        expected_uuid: List[str],
        page_limit: int = CONFIG.page_limit,
        expect_id: bool = False,
        expected_as_is: bool = False,
    ):
        # Sort by immutable_id
        if "sort=" not in request:
            request += "&sort=immutable_id"

        response = get_good_response(request=request)

        if expect_id:
            response_uuids = [struct["id"] for struct in response["data"]]
        else:
            # Expect UUIDs (immutable_id)
            response_uuids = [
                struct["attributes"]["immutable_id"] for struct in response["data"]
            ]
        assert response["meta"]["data_returned"] == len(expected_uuid)

        if not expected_as_is:
            expected_uuid = sorted(expected_uuid)

        if len(expected_uuid) > page_limit:
            assert expected_uuid[:page_limit] == response_uuids
        else:
            assert expected_uuid == response_uuids

    return _check_response


@pytest.fixture
def check_error_response(client):
    """General method for testing expected errornous response"""

    def _check_error_response(
        request: str,
        expected_status: int = None,
        expected_title: str = None,
        expected_detail: str = None,
    ):
        response = None
        try:
            response = client.get(request)
            assert response.status_code == expected_status, (
                "Request should have been an error with status code "
                f"{expected_status}, but instead {response.status_code} was received."
                f"\nResponse:\n{response.json()}",
            )

            response = response.json()
            assert len(response["errors"]) == 1, response.get(
                "errors", "'errors' not found"
            )
            assert response["meta"]["data_returned"] == 0, response.get(
                "meta", "'meta' not found"
            )

            error = response["errors"][0]
            assert str(expected_status) == error["status"], error
            assert expected_title == error["title"], error

            if expected_detail is None:
                expected_detail = "Error trying to process rule "
                assert error["detail"].startswith(expected_detail), error
            else:
                assert expected_detail == error["detail"], error

        except Exception as exc:
            print("Request attempted:")
            print(f"{client.base_url}{client.version}{request}")
            if response:
                print(f"\nCaptured response:\n{response}")
            raise exc

    return _check_error_response
