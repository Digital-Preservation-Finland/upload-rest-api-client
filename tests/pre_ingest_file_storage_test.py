"""Unit tests for `pre_ingest_file_storage` module."""

import pytest

from upload_rest_api_client import __version__
from upload_rest_api_client.pre_ingest_file_storage import (
    PreIngestFileStorage, PreIngestFileNotFoundError
)


def _filter_qs(qs):
    """Filter a query parameter dict to remove the session and request
    identifiers
    """
    qs = qs.copy()

    del qs["_request_id"]
    del qs["_session_id"]

    return qs


@pytest.mark.parametrize(
    ('target', 'result'),
    [
        (
            '/',
            [
                {'parent_directory_identifier': 'foo1',
                 'identifier': 'foo3',
                 'checksum': 'bar3',
                 'path': '/files1'},
                {'parent_directory_identifier': 'foo2',
                 'identifier': 'foo4',
                 'checksum': 'bar4',
                 'path': '/directory1/files2'}
            ]
        ),
        (
            '/directory1',
            [
                {'parent_directory_identifier': 'foo2',
                 'identifier': 'foo4',
                 'checksum': 'bar4',
                 'path': '/directory1/files2'}
            ]
        ),
    ]
)
def test_directory_files(requests_mock, target, result):
    """Test `directory_files` method.

    :param requests_mock:
    :param target: target directory
    :param result: result returned from tested method

    """
    # Mock upload-rest-api responses as upload-rest-api would contain
    # two files:
    #     /file1
    #     /directory1/file2
    requests_mock.get(
        'http://localhost/v1/files/test_project?all=true',
        json={"/": ["file1"], "/directory1": ["file2"]},
        additional_matcher=lambda req: _filter_qs(req.qs) == {"all": ["true"]}
    )

    requests_mock.get(
        'http://localhost/v1/files/test_project/',
        json={
            "directories": ["directory1"],
            "files": ["file1"],
            "identifier": "foo1"
        },
        additional_matcher=lambda req: _filter_qs(req.qs) == {}
    )

    requests_mock.get('http://localhost/v1/files/test_project/directory1',
                      json={"directories": [],
                            "files": ["file2"],
                            "identifier": "foo2"})

    requests_mock.get('http://localhost/v1/files/test_project/file1',
                      json={"file_path": "/files1",
                            "identifier": 'foo3',
                            "md5": "bar3",
                            "timestamp": "2021-06-21T12:45:28+00:00"})

    requests_mock.get(
        'http://localhost/v1/files/test_project/directory1/file2',
        json={
            "file_path": "/directory1/files2",
            "identifier": 'foo4',
            "md5": "bar4",
            "timestamp": "2021-06-21T12:45:28+00:00"
        }
    )

    # Test the method for targe directory
    client = PreIngestFileStorage(
        False,
        {
            'host': 'http://localhost',
            'user': 'testuser',
            'password': 'password',
            'token': ''
        }
    )
    assert client.directory_files('test_project', target) == result


def test_browsing_nonexistent_file(requests_mock):
    """Test that browsing a file that does not exist raises a
    FileNotFoundError.

    :param requests_mock: HTTP requests mocker
    """
    host = "http://localhost"
    project = "test_project"
    path = "invalid_filepath"

    requests_mock.get(
        f"{host}/v1/files/{project}/{path}",
        json={"status": 404, "error": "File not found"},
        headers={"content-type": "application/json"},
        status_code=404
    )

    client = PreIngestFileStorage(
        False,
        {
            "host": host,
            "user": "testuser",
            "password": "password",
            "token": ""
        }
    )

    with pytest.raises(PreIngestFileNotFoundError) as error:
        client.browse(project, path)
        assert "File not found" in str(error.value)


def test_delete(requests_mock):
    """Test deleting resources from pre-ingest file-storage.

    :param requests_mock: HTTP requests mocker
    """
    host = "http://localhost"

    response = {
        "file_path": "/filepath",
        "message": "deleted",
        "metax": {
            "deleted_files_count": 1
        }
    }

    adapter = requests_mock.delete(
        f"{host}/v1/files/test_project/filepath",
        json=response,
        status_code=200
    )

    client = PreIngestFileStorage(
        False,
        {
            "host": host,
            "user": "",
            "password": "",
            "token": "test_token"
        }
    )

    client.delete("test_project", "filepath")
    assert adapter.called


def test_task_status(requests_mock):
    """Test task status method.

    :param requests_mock: HTTP requests mocker
    """
    host = "http://localhost"

    task_response = {"status": "error", "message": "error message"}
    requests_mock.get("/v1/tasks/1", json=task_response)

    client = PreIngestFileStorage(
        False,
        {
            "host": host,
            "user": "",
            "password": "",
            "token": "test_token"
        }
    )

    task = client.task_status('1')
    assert task['status'] == 'error'
    assert task['message'] == 'error message'


def test_user_agent_header():
    """Test that requests have User-Agent header set with client version
    details.
    """
    client = PreIngestFileStorage(
        False,
        {
            "host": "http://localhost",
            "user": "",
            "password": "",
            "token": "test_token"
        }
    )

    assert "User-Agent" in client.session.headers.keys()

    user_agent = client.session.headers["User-Agent"]
    assert f"upload-rest-api-client/{__version__}" in user_agent
