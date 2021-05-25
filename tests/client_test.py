"""Unit tests for `client` module."""

import tarfile

import pytest

import upload_rest_api_client.client

API_URL = 'http://localhost/v1'


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


@pytest.mark.parametrize(
    ('optional_arguments', 'output'),
    [
        (
            ['--target', 'target'],
            ['Generated metadata for directory: /target',
             'Directory identifier: directory_id1']
        ),
        (
            ['--target', 'target', '--format', 'files'],
            ['Generated file metadata',
             '']
            + ["%45s    %45s    %32s    %s" % line for line in (
                ("parent_dir", "identifier", "checksum_value", "file_path"),
                ('directory_id1', 'file_id1', 'checksum1', '/target/file1'),
                ('directory_id1', 'file_id2', 'checksum2', '/target/file2')
            )]
        )
    ]
)
@pytest.mark.usefixtures('mock_configuration')
def test_upload_archive(requests_mock, capsys, tmp_path, optional_arguments,
                        output):
    """Test uploading archive.

    Test that HTTP requests are sent to correct urls, and command
    produces expected output.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    :param tmp_path: temporary path for archive file
    :param optional_arguments: list of optional commandline arguments
    :param output: list of expected command output lines
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

    # Create archive that contains two files
    file1 = tmp_path / 'file1'
    file1.write_text('foo')
    file2 = tmp_path / 'file2'
    file2.write_text('bar')
    archive = tmp_path / 'archive.tar'
    with tarfile.open(archive, 'w') as open_archive:
        open_archive.add(file1)
        open_archive.add(file2)

    # Post archive to "target" directory
    upload_rest_api_client.client.main(['upload', str(archive)]
                                       + optional_arguments)

    # Check output line by line, but skip first line
    captured = capsys.readouterr().out.splitlines()
    assert captured[1:] == output


@pytest.mark.usefixtures('mock_configuration')
def test_upload_archive_to_root(requests_mock, tmp_path):
    """Test uploading archive to root directory.

    Test that HTTP requests are sent to correct urls when no target
    directory is given as argument.

    :param requests_mock: HTTP request mocker
    :param capsys: captured command output
    :param tmp_path: temporary path for archive file
    :param output_argument: commandline argument to choose output format
    :param output: list of expected command output lines
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

    # Create archive that contains two files
    file1 = tmp_path / 'file1'
    file1.write_text('foo')
    file2 = tmp_path / 'file2'
    file2.write_text('bar')
    archive = tmp_path / 'archive.tar'
    with tarfile.open(archive, 'w') as open_archive:
        open_archive.add(file1)
        open_archive.add(file2)

    # Post archive to root directory
    upload_rest_api_client.client.main(['upload', str(archive)])
