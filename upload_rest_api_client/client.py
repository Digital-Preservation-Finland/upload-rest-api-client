"""upload-rest-api-client"""
import os
import configparser
import argparse
import tarfile
import zipfile
import hashlib

import requests
import argcomplete
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


def _parse_args():
    """Parse command line arguments."""
    # Base parser
    parser = argparse.ArgumentParser(
        description="Client for accessing pre-ingest file storage."
    )
    parser.add_argument(
        "-k", "--insecure",
        default=False, action="store_true",
        help="skip SSL certification check"
    )
    subparsers = parser.add_subparsers(title="command")

    # Upload parser
    upload_parser = subparsers.add_parser(
        "upload", help="upload package to the pre-ingest file storage"
    )
    upload_parser.add_argument(
        "filepath",
        help="path to the uploaded tar or zip archive"
    )
    upload_parser.set_defaults(func=_upload)

    # Setup bash auto completion
    argcomplete.autocomplete(parser)

    return parser.parse_args()


def _upload(args):
    """Upload tar or zip archive to the pre-ingest file storage and generate
    Metax metadata.
    """
    # Check that the provided file is either a zip or tar archive
    fpath = args.filepath
    if not tarfile.is_tarfile(fpath) and not zipfile.is_zipfile(fpath):
        raise ValueError("Unsupported file: '%s'" % fpath)

    verify = not args.insecure
    host, user, password = _parse_conf_file()
    auth = HTTPBasicAuth(user, password)
    files_api = "%s/filestorage/api/v1/files" % host
    metadata_api = "%s/filestorage/api/v1/metadata" % host
    file_checksum = _md5_digest(fpath)

    # Upload the package
    with open(fpath, "rb") as upload_file:
        response = requests.post(
            "%s/upload.zip" % files_api,
            data=upload_file,
            auth=auth,
            verify=verify
        )
        response.raise_for_status()

    if response.json()["md5"] != file_checksum:
        raise DataIntegrityError("Checksums do not match")

    print("Uploaded '%s'" % fpath)

    # Generate file metadata
    response = requests.post(
        "%s/*" % metadata_api,
        auth=auth,
        verify=verify
    )
    response.raise_for_status()

    print("Generated file metadata\n")
    print_format = "%10s    %50s    %45s    %32s"
    print(print_format % (
        "parent_dir",
        "file_path",
        "identifier",
        "checksum_value"
    ))
    for _file_md in response.json()["metax_response"]["success"]:
        print(print_format % (
            _file_md["object"]["parent_directory"]["id"],
            _file_md["object"]["file_path"],
            _file_md["object"]["identifier"],
            _file_md["object"]["checksum_value"]
        ))


def main():
    """Parse command line arguments and run the chosen function."""
    args = _parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
