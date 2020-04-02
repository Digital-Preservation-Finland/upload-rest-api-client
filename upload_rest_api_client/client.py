"""upload-rest-api-client"""
from __future__ import print_function
import os
import sys
import json
import configparser
import argparse
import tarfile
import zipfile
import hashlib
from time import sleep

import requests
import argcomplete
import urllib3
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError


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


def _parse_conf_file(conf):
    """Parse configuration file.

    :param conf: Path to the configuration file
    :returns: host, username, password
    """
    configuration = configparser.ConfigParser()
    configuration.read(os.path.expanduser(conf))
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
    parser.add_argument(
        "-o", "--output",
        help="Path to the file created identifiers are written"
    )
    parser.add_argument(
        "-c", "--config", default="~/.upload.cfg",
        help=(
            "Path to the configuration file. Configuration file should include "
            "host, username and password in section [upload]."
        )
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


def _wait_response(response, auth, verify):
    status = "pending"
    polling_url = response.json()["polling_url"]

    while status == "pending":
        sleep(5)
        print('.', end='', flush=True)
        response = requests.get(polling_url, auth=auth, verify=verify)
        try:
            response.raise_for_status()
        except HTTPError:
            if response.headers["content-type"] == "application/json":
                print(json.dumps(response.json(), indent=4))
                sys.exit(1)
            raise
        status = response.json()['status']

    print()
    if status == "error":
        print(json.dumps(response.json(), indent=4))
        sys.exit(1)

    return response


def _upload(args):
    """Upload tar or zip archive to the pre-ingest file storage and generate
    Metax metadata.
    """
    # Check that the provided file is either a zip or tar archive
    fpath = args.filepath
    if not tarfile.is_tarfile(fpath) and not zipfile.is_zipfile(fpath):
        raise ValueError("Unsupported file: '%s'" % fpath)

    # Check that the configuration file exists
    if not os.path.isfile(os.path.expanduser(args.config)):
        raise ValueError("Config file '%s' not found" % args.config)

    verify = not args.insecure
    host, user, password = _parse_conf_file(args.config)
    auth = HTTPBasicAuth(user, password)
    archives_api = "%s/filestorage/api/v1/archives" % host
    metadata_api = "%s/filestorage/api/v1/metadata" % host
    file_checksum = _md5_digest(fpath)

    # Upload the package
    with open(fpath, "rb") as upload_file:
        response = requests.post(
            archives_api,
            data=upload_file,
            auth=auth,
            verify=verify
        )

        # Print InsecureRequestWarning only once
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        try:
            response.raise_for_status()
        except HTTPError:
            if response.headers["content-type"] == "application/json":
                print(json.dumps(response.json(), indent=4))
                return
            raise

    if response.status_code == 202:
        response = _wait_response(response, auth, verify)
    if response.json()["md5"] != file_checksum:
        raise DataIntegrityError("Checksums do not match")
    print("Uploaded '%s'" % fpath)

    # Generate file metadata
    response = requests.post(
        "%s/*" % metadata_api,
        auth=auth,
        verify=verify
    )
    try:
        response.raise_for_status()
    except HTTPError:
        if response.headers["content-type"] == "application/json":
            print(json.dumps(response.json(), indent=4))
            return

        raise
    if response.status_code == 202:
        response = _wait_response(response, auth, verify)
    print("Generated file metadata\n")
    print_format = "%45s    %45s    %32s    %s"
    print(print_format % (
        "parent_dir",
        "identifier",
        "checksum_value",
        "file_path"
    ))
    for _file_md in response.json()["metax_response"]["success"]:
        print(print_format % (
            _file_md["object"]["parent_directory"]["identifier"],
            _file_md["object"]["identifier"],
            _file_md["object"]["checksum"]["value"],
            _file_md["object"]["file_path"]
        ))

    if args.output:
        with open(args.output, "w") as f_out:
            for _file_md in response.json()["metax_response"]["success"]:
                f_out.write("%s\t%s\t%s\t%s\n" % (
                    _file_md["object"]["parent_directory"]["identifier"],
                    _file_md["object"]["identifier"],
                    _file_md["object"]["checksum"]["value"],
                    _file_md["object"]["file_path"]
                ))


def main():
    """Parse command line arguments and run the chosen function."""
    args = _parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
