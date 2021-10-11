"""Unit tests for `pre_ingest_file_storage` module."""

import pytest

from upload_rest_api_client.pre_ingest_file_storage import PreIngestFileStorage


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
        complete_qs=True
    )

    requests_mock.get(
        'http://localhost/v1/files/test_project/',
        json={
            "directories": ["directory1"],
            "files": ["file1"],
            "identifier": "foo1"
        },
        complete_qs=True
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
    client = PreIngestFileStorage(False,
                                  'http://localhost',
                                  'testuser',
                                  'password')
    assert client.directory_files('test_project', target) == result
