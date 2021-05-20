"""Pytest fixture functions."""
import upload_rest_api_client.client
import pytest


@pytest.fixture(scope="function")
def mock_configuration(monkeypatch):
    """Patch upload_rest_api configuration parsing."""
    monkeypatch.setattr(
        upload_rest_api_client.client, "_parse_conf_file",
        lambda conf: ("http://localhost", "testuser", "password")
    )