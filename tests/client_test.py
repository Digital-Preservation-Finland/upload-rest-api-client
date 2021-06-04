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
    requests_mock.get(f"{API_URL}/files/", json=response)
    upload_rest_api_client.client.main(['browse', '/'])
    captured = capsys.readouterr()
    assert captured.out == output


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
    requests_mock.post(f'{API_URL}/archives')
    requests_mock.post(f'{API_URL}/metadata/target*')
    requests_mock.get(f'{API_URL}/files/target',
                      json={
                          "directories": [],
                          "files": [
                              "file1",
                              "file2",
                          ],
                          "identifier": 'directory_id1'
                      })
    requests_mock.get(f'{API_URL}/files/',
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
    requests_mock.get(f'{API_URL}/files/target/file1',
                      json={
                          "file_path": "/target/file1",
                          "metax_identifier": "file_id1",
                          "md5": "checksum1"
                      })
    requests_mock.get(f'{API_URL}/files/target/file2',
                      json={
                          "file_path": "/target/file2",
                          "metax_identifier": "file_id2",
                          "md5": "checksum2"
                      })

    # Post archive to "target" directory
    upload_rest_api_client.client.main(['upload', str(archive),
                                        '--target', 'target'])

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
    requests_mock.post(f'{API_URL}/archives')
    requests_mock.post(f'{API_URL}/metadata/*')
    requests_mock.get(f'{API_URL}/files/',
                      json={
                          "directories": [],
                          "files": [
                              "file1",
                              "file2",
                          ],
                          "identifier": 'directory_id1'
                      })

    # Post archive to root directory
    upload_rest_api_client.client.main(['upload', str(archive)]+arguments)

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
    requests_mock.post(f'{API_URL}/archives')
    requests_mock.post(f'{API_URL}/metadata/*')
    requests_mock.get(f'{API_URL}/files/',
                      json={
                          "directories": ['dir1', 'dir2'],
                          "files": [],
                          "identifier": 'directory_id0'
                      })
    requests_mock.get(f'{API_URL}/files/dir1',
                      json={
                          "directories": [],
                          "files": ['file1'],
                          "identifier": 'directory_id1'
                      })
    requests_mock.get(f'{API_URL}/files/dir2',
                      json={
                          "directories": [],
                          "files": ['file1'],
                          "identifier": 'directory_id2'
                      })

    # Post archive to root directory
    upload_rest_api_client.client.main(['upload', str(directory_archive)])

    # Check output
    assert capsys.readouterr().out \
        == (f"Uploaded '{str(directory_archive)}'\n"
            "Generated metadata for directory: /\n"
            "\n"
            "The directory contains subdirectories:\n"
            "dir1 (identifier: directory_id1)\n"
            "dir2 (identifier: directory_id2)\n")
