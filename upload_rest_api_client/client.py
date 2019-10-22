"""upload-rest-api-client"""
import os
import sys
import configparser
import tarfile
import zipfile
import hashlib

import requests
from requests.auth import HTTPBasicAuth


class DataIntegrityError(Exception):
    """Exception raised when data corruption occurs during a transfer."""


def _md5_digest(fpath):
    """Return md5 digest of file fpath

    :param fpath: path to file to be hashed
    :returns: digest as a string
    """
    md5_hash = hashlib.md5()
    with open(fpath, "rb") as _file:
        # read the file in 1MB chunks
        for chunk in iter(lambda: _file.read(1024 * 1024), b''):
            md5_hash.update(chunk)

    return md5_hash.hexdigest()


def _parse_conf_file():
    """Parse configuration from ~/.upload.cfg

    :returns: host, username, password
    """
    configuration = configparser.ConfigParser()
    configuration.read(os.path.expanduser("~/.upload.cfg"))
    return (
        configuration.get("upload", "host"),
        configuration.get("upload", "user"),
        configuration.get("upload", "password"),
    )


def _read_command_line_args():
    """Check that the package is either a zipfile or a tarfile"""
    if len(sys.argv) == 1:
        raise ValueError("Upload package not defined")

    fpath = sys.argv[-1]
    if not tarfile.is_tarfile(fpath) and not zipfile.is_zipfile(fpath):
        raise ValueError("Unsupported file: '%s'" % fpath)

    return fpath


def main():
    """Upload files and generate the file metadata"""
    host, user, password = _parse_conf_file()
    auth = HTTPBasicAuth(user, password)
    files_api = "filestorage/api/v1/files/"
    metadata_api = "filestorage/api/v1/metadata/"
    upload_package = _read_command_line_args()
    upload_checksum = _md5_digest(upload_package)

    # Upload the package
    with open(upload_package, "rb") as upload_file:
        response = requests.post(
            files_api,
            input_stream=upload_file,
            auth=auth
        )
        response.raise_for_status()

    if response.json()["md5"] != upload_checksum:
        raise DataIntegrityError("Checksums do not match")

    print("Uploaded '%s'" % upload_package)

    # Generate file metadata
    response = requests.post("%s*" % metadata_api, auth=auth)
    response.raise_for_status()

    print("Generated file metadata\n")
    for _file_md in response.json()["metax_response"]["success"]:
        print(
            _file_md["object"]["file_path"],
            _file_md["object"]["identifier"],
            _file_md["object"]["checksum_value"],
            "parent_dir_id=%s" % _file_md["object"]["parent_directory"]["id"]
        )
