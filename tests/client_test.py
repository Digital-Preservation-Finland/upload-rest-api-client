"""Unit tests for `client` module."""

import tarfile

import pytest

import upload_rest_api_client.client

API_URL = 'http://localhost/v1'


@pytest.fixture(scope='function', name='archive')
def sample_file_archive(tmp_path):
    """Create a sample tar archive in temporary directory.

    The archive contains two files.

    :tmp_path: temporary directory
    :returns: path to tar archive
    """
    file1 = tmp_path / 'file1'
    file1.write_text('foo')
    file2 = tmp_path / 'file2'
    file2.write_text('bar')
    tmp_archive = tmp_path / 'archive.tar'
    with tarfile.open(tmp_archive, 'w') as open_archive:
        open_archive.add(file1)
        open_archive.add(file2)

    return tmp_archive


@pytest.fixture(scope='function', name='directory_archive')
def sample_directory_archive(tmp_path):
    """Create a sample tar archive in temporary directory.

    The archive contains two directories.

    :tmp_path: temporary directory
    :returns: path to tar archive
    """
    file1 = tmp_path / 'dir1' / 'file1'
    file1.parent.mkdir()
    file1.write_text('foo')
    file2 = tmp_path / 'dir2' / 'file2'
    file2.parent.mkdir()
    file2.write_text('bar')
    tmp_archive = tmp_path / 'archive.tar'
    with tarfile.open(tmp_archive, 'w') as open_archive:
        open_archive.add(file1)
        open_archive.add(file2)

    return tmp_archive


@pytest.mark.parametrize(
    "config",
    [
        {
            "host": "http://localhost",
            "user": "",
            "password": "",
            "token": "fddps-fake-token"
        },
        {
            "host": "http://localhost",
            "user": "test_user",
            "password": "test_password",
            "token": ""
        }
    ]
)
def test_authentication(monkeypatch, requests_mock, config):
    """
    Test that correct headers are sent in requests depending on what
    authentication method is configured.
    """
    monkeypatch.setattr(
        upload_rest_api_client.client, "_parse_conf_file",
        lambda conf: config
    )

    requests_mock.get(
        f"{API_URL}/users/projects",
        json={
            "projects": [
                {
                    "identifier": "test_project_a",
                    "used_quota": 1024,
                    "quota": 1024000
                },
                {
                    "identifier": "test_project_b",
                    "used_quota": 4096,
                    "quota": 4096000
                }
            ]
        }
    )

    upload_rest_api_client.client.main(['list-projects'])

    # Inspect the request
    last_request = requests_mock.request_history[0]

    if config["token"]:
        # Bearer authentication used
        assert last_request.headers["Authorization"] == \
            "Bearer fddps-fake-token"
    else:
        # Basic authentication used
        assert last_request.headers["Authorization"] == \
            "Basic dGVzdF91c2VyOnRlc3RfcGFzc3dvcmQ="


@pytest.mark.usefixtures('mock_configuration')
@pytest.mark.parametrize(
    ("response", "output"),
    [
        # Directory that contains multiple directories and files
        (
            {
                "directories": [
                    "dir1",
                    "dir2"
                ],
                "files": [
                    "file1",
                    "file2",
                    "file3"
                ],
                "identifier": 'testidentifier'
            },
            ("directories:\n"
             "    dir1\n"
             "    dir2\n"
             "\n"
             "files:\n"
             "    file1\n"
             "    file2\n"
             "    file3\n"
             "\n"
             "identifier:\n"
             "    testidentifier\n\n"),
        ),
        # Empty directory
        (
            {
                "directories": [],
                "files": [],
                "identifier": 'testidentifier'
            },
            ("directories:\n"
             "\n"
             "files:\n"
             "\n"
             "identifier:\n"
             "    testidentifier\n\n"),
        ),
        # Directory without identifier
        (
            {
                "directories": [],
                "files": [],
                "identifier": None
            },
            ("directories:\n"
             "\n"
             "files:\n"
             "\n"
             "identifier:\n"
             "    None\n\n"),
        ),
        # A file
        (
            {
                "file_path": "foo",
                "md5": "bar",
                "identifier": "baz",
                "timestamp": "2021-06-21T12:45:28+00:00"
            },
            ("file_path:\n"
             "    foo\n"
             "\n"
             "md5:\n"
             "    bar\n"
             "\n"
             "identifier:\n"
             "    baz\n"
             "\n"
             "timestamp:\n"
             "    2021-06-21T12:45:28+00:00\n\n")
        ),
        # A file without identifier
        (
            {
                "file_path": "foo",
                "md5": "bar",
                "identifier": None,
                "timestamp": "2021-06-21T12:45:28+00:00"
            },
            ("file_path:\n"
             "    foo\n"
             "\n"
             "md5:\n"
             "    bar\n"
             "\n"
             "identifier:\n"
             "    None\n"
             "\n"
             "timestamp:\n"
             "    2021-06-21T12:45:28+00:00\n\n")
        )
    ]
)
def test_browse(requests_mock, capsys, response, output):
    """Test browse command.

    Test that HTTP requests are sent to correct urls, and command
    produces expected output.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    :param response: JSON resposne from API
    :param output: list of expected command output lines
    """
    requests_mock.get(f"{API_URL}/files/test_project/some_path", json=response)
    upload_rest_api_client.client.main(
        ['browse', '--project', 'test_project', '/some_path']
    )
    captured = capsys.readouterr()
    assert captured.out == output


@pytest.mark.usefixtures('mock_configuration')
def test_browsing_nonexistent_file(requests_mock, capsys):
    """Test that using browse command for a nonexistent file shows a sensible
    error message.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    """
    project = "test_project"
    path = "invalid_filepath"

    requests_mock.get(
        f"{API_URL}/files/{project}/{path}",
        json={"status": 404, "error": "File not found"},
        headers={"content-type": "application/json"},
        status_code=404
    )

    upload_rest_api_client.client.main(
        ['browse', '--project', project, path]
    )
    captured = capsys.readouterr()
    assert "File not found\n" == captured.out


@pytest.mark.usefixtures("mock_configuration")
def test_browse_default_project(requests_mock, capsys):
    """Test that browse command uses the default project if found in the
    configuration file
    """
    requests_mock.get(
        f"{API_URL}/files/default_test_project/some_path",
        json={
            "directories": [],
            "files": [],
            "identifier": 'testidentifier'
        }
    )
    upload_rest_api_client.client.main(
        ['browse', '/some_path']
    )
    captured = capsys.readouterr()
    assert (
        "directories:\n\nfiles:\n\nidentifier:\n    testidentifier\n\n"
        in captured.out
    )


@pytest.mark.usefixtures('mock_configuration')
def test_upload_archive(requests_mock, capsys, archive):
    """Test uploading archive.

    Test that HTTP requests are sent to correct urls, and command
    produces expected output.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    :param archive: path to sample archive file
    """
    # Mock all urls that are requested
    requests_mock.post(f'{API_URL}/archives/test_project')
    requests_mock.post(f'{API_URL}/metadata/test_project/target')
    requests_mock.get(f'{API_URL}/files/test_project/target',
                      json={
                          "directories": [],
                          "files": [
                              "file1",
                              "file2",
                          ],
                          "identifier": 'directory_id1'
                      })
    requests_mock.get(f'{API_URL}/files/test_project',
                      json={
                          "directories": ['target'],
                          "files": [],
                          "identifier": 'directory_id2'
                      })
    requests_mock.get(f'{API_URL}/files',
                      json={
                          "/": [],
                          "/target": ['file1', 'file2']
                      })
    requests_mock.get(f'{API_URL}/files/test_project/target/file1',
                      json={
                          "file_path": "/target/file1",
                          "identifier": "file_id1",
                          "md5": "checksum1"
                      })
    requests_mock.get(f'{API_URL}/files/test_project/target/file2',
                      json={
                          "file_path": "/target/file2",
                          "identifier": "file_id2",
                          "md5": "checksum2"
                      })

    # Post archive to "target" directory
    upload_rest_api_client.client.main([
        'upload', '--project', 'test_project', str(archive),
        '--target', 'target'
    ])

    # Check output
    assert capsys.readouterr().out \
        == (f"Uploaded '{str(archive)}'\n"
            "Generated metadata for directory: /target "
            "(identifier: directory_id1)\n")


@pytest.mark.usefixtures('mock_configuration')
@pytest.mark.parametrize(
    'arguments',
    (
        [],
        ['--target', '/']
    )
)
def test_upload_archive_to_root(requests_mock, capsys, archive, arguments):
    """Test uploading archive to root directory.

    Test that HTTP requests are sent to correct urls when root directory
    is used as target directory.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    :param archive: path to sample archive file
    :param arguments: extra commandline arguments
    """
    # Mock all urls that are requested
    requests_mock.post(f'{API_URL}/archives/test_project')
    requests_mock.post(f'{API_URL}/metadata/test_project/*')
    requests_mock.get(f'{API_URL}/files/test_project/',
                      json={
                          "directories": [],
                          "files": [
                              "file1",
                              "file2",
                          ],
                          "identifier": 'directory_id1'
                      })

    # Post archive to root directory
    upload_rest_api_client.client.main(
        ['upload', '--project', 'test_project', str(archive)] + arguments
    )

    # Check output
    assert capsys.readouterr().out \
        == (f"Uploaded '{str(archive)}'\n"
            "Generated metadata for directory: /\n")


@pytest.mark.usefixtures('mock_configuration')
def test_upload_archive_with_directories(requests_mock, capsys,
                                         directory_archive):
    """Test uploading archive that contains directories.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    :param directory_archive: path to sample archive file
    """
    # Mock all urls that are requested
    requests_mock.post(f'{API_URL}/archives/test_project')
    requests_mock.post(f'{API_URL}/metadata/test_project/*')
    requests_mock.get(f'{API_URL}/files/test_project/',
                      json={
                          "directories": ['dir1', 'dir2'],
                          "files": [],
                          "identifier": 'directory_id0'
                      })
    requests_mock.get(f'{API_URL}/files/test_project/dir1',
                      json={
                          "directories": [],
                          "files": ['file1'],
                          "identifier": 'directory_id1'
                      })
    requests_mock.get(f'{API_URL}/files/test_project/dir2',
                      json={
                          "directories": [],
                          "files": ['file1'],
                          "identifier": 'directory_id2'
                      })

    # Post archive to root directory
    upload_rest_api_client.client.main(
        ['upload', '--project', 'test_project', str(directory_archive)]
    )

    # Check output
    assert capsys.readouterr().out \
        == (f"Uploaded '{str(directory_archive)}'\n"
            "Generated metadata for directory: /\n"
            "\n"
            "The directory contains subdirectories:\n"
            "dir1 (identifier: directory_id1)\n"
            "dir2 (identifier: directory_id2)\n")


@pytest.mark.usefixtures('mock_configuration')
def test_upload_default_project(requests_mock, capsys,
                                directory_archive):
    """
    Test running the `upload` command and ensure that the default project is
    used.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    :param directory_archive: path to sample archive file
    """
    # Mock all urls that are requested
    requests_mock.post(f'{API_URL}/archives/default_test_project')
    requests_mock.post(f'{API_URL}/metadata/default_test_project/*')
    requests_mock.get(f'{API_URL}/files/default_test_project/',
                      json={
                          "directories": [],
                          "files": [],
                          "identifier": 'directory_id0'
                      })

    # Post archive to root directory
    upload_rest_api_client.client.main(
        ['upload', str(directory_archive)]
    )

    # Check output
    assert capsys.readouterr().out \
        == (f"Uploaded '{str(directory_archive)}'\n"
            "Generated metadata for directory: /\n")


@pytest.mark.usefixtures('mock_configuration')
def test_list_projects(requests_mock, capsys):
    """Test list projects
    """
    requests_mock.get(
        f"{API_URL}/users/projects",
        json={
            "projects": [
                {
                    "identifier": "test_project_a",
                    "used_quota": 1024,
                    "quota": 1024000
                },
                {
                    "identifier": "test_project_b",
                    "used_quota": 4096,
                    "quota": 4096000
                }
            ]
        }
    )

    upload_rest_api_client.client.main(['list-projects'])

    out = capsys.readouterr().out

    # Header shown in correct order
    assert out.index("Project") < out.index("Used quota") < out.index("Quota")

    assert (
        out.index("test_project_a")
        < out.index("1024")
        < out.index("1024000")
    )
    assert (
        out.index("test_project_b")
        < out.index("4096")
        < out.index("4096000")
    )


@pytest.mark.parametrize("args", [
    ["upload", "test_file.tar"],
    ["browse", "test_project"]
])
def test_command_no_default_project_provided(monkeypatch, capsys, args):
    """
    Test that the client will print a warning if no project name is provided
    and no default is configured.
    """
    monkeypatch.setattr(
        upload_rest_api_client.client, "_parse_conf_file",
        lambda conf: {
            "host": "http://localhost",
            "user": "testuser",
            "password": "password",
            "token": "",
            "default_config": ""
        }
    )

    with pytest.raises(RuntimeError) as exc:
        upload_rest_api_client.client.main(args)

    assert "Project name was not provided" in str(exc.value)


@pytest.mark.usefixtures('mock_configuration')
def test_delete(requests_mock, capsys):
    """Test delete command.

    Test that HTTP requests are sent to correct urls, and command
    produces expected output.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    """
    response = {
        "file_path": "/test_path.txt",
        "message": "deleted",
        "metax": {
            "deleted_files_count": 1
        }
    }

    requests_mock.delete(
        f"{API_URL}/files/test_project/test_path.txt",
        json=response,
        status_code=200
    )
    upload_rest_api_client.client.main(
        ["delete", "--project", "test_project", "/test_path.txt"]
    )
    captured = capsys.readouterr()
    assert captured.out == (
        "Deleted '/test_path.txt' and all associated metadata.\n")
