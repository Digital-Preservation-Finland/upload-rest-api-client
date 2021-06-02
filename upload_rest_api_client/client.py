"""upload-rest-api-client."""
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


def _md5_digest(fpath):
    """Return md5 digest of file fpath.

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
    if not os.path.isfile(os.path.expanduser(conf)):
        raise ValueError(f"Config file '{conf}' not found")

    configuration = configparser.ConfigParser()
    configuration.read(os.path.expanduser(conf))
    return (
        configuration.get("upload", "host"),
        configuration.get("upload", "user"),
        configuration.get("upload", "password"),
    )


def _parse_args(cli_args):
    """Parse command line arguments.

    :param cli_args: command line arguments
    :returns: parsed command line arguments
    """
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
        "-c", "--config", default="~/.upload.cfg",
        help=(
            "Path to the configuration file. Configuration file should "
            "include host, username and password in section [upload]."
        )
    )
    parser.set_defaults(func=None)
    subparsers = parser.add_subparsers(title="command")

    # Browse parser
    browse_parser = subparsers.add_parser(
        "browse", help="browse files in pre-ingest file storage"
    )
    browse_parser.add_argument(
        "path",
        help="path to file or directory"
    )
    browse_parser.set_defaults(func=_browse)

    # Upload parser
    upload_parser = subparsers.add_parser(
        "upload", help="upload package to the pre-ingest file storage"
    )
    upload_parser.add_argument(
        "source",
        help="path to the uploaded tar or zip archive"
    )
    upload_parser.add_argument(
        "--target",
        help="directory where the uploaded archive is extracted",
        default='/'
    )
    upload_parser.add_argument(
        "-o", "--output",
        help="Path to the file where created identifiers are written"
    )
    upload_parser.set_defaults(func=_upload)

    # Setup bash auto completion
    argcomplete.autocomplete(parser)

    # Parse arguments
    args = parser.parse_args(cli_args)
    if not args.func:
        parser.print_help()
        sys.exit(0)
    return args


def _browse(client, args):
    """Browse pre-ingest storage using client.

    Parse command specific command-line arguments and use provided
    client to browse pre-ingest file storage.

    :param client: Pre-ingest file storage client
    :param args: Browsing arguments
    """
    resource = client.browse(args.path)
    for key, value in resource.items():
        print(f"{key}:")
        value_list = value if isinstance(value, list) else [value]
        for value_ in value_list:
            print(f"    {value_}")
        print("")


def _upload(client, args):
    """Upload archive to pre-ingest storage using client.

    Parse command specific command-line arguments and use provided
    client to browse pre-ingest file storage.

    :param client: Pre-ingest file storage client
    :param args: Upload arguments
    """
    # Ensure that target directory path starts with slash
    target = "/{}".format(args.target.strip('/'))

    # Upload archive
    client.upload_archive(args.source, target)
    print(f"Uploaded '{args.source}'")

    # Generate metadata
    directory = client.generate_directory_metadata(target)

    if args.output:
        files = client.directory_files(target)
        with open(args.output, "w") as f_out:
            for file_ in files:
                f_out.write("{}\t{}\t{}\t{}\n".format(*file_.values()))

    print("Generated metadata for directory: {} (identifier: {})".format(
        target, directory['identifier']
    ))

    if directory['directories']:
        # Print list of subdirectories
        print("\nThe directory contains subdirectories:")
        for subdirectory in directory['directories']:
            identifier \
                = client.browse(f"{target}/{subdirectory}")["identifier"]
            print(f"{subdirectory} (identifier: {identifier})")


class PreIngestFileStorage():
    """Pre-ingest file storage client."""

    def __init__(self, verify, host, user, password):
        """Initialize pre-ingest file storage client.

        :param bool verify: Use SSL verification
        :param host: API url
        :param user: username
        :param password: password
        """
        self.session = requests.Session()
        self.session.verify = verify
        self.session.auth = (HTTPBasicAuth(user, password))
        self.session.mount(host, requests.adapters.HTTPAdapter(max_retries=5))
        self.archives_api = f"{host}/v1/archives"
        self.metadata_api = f"{host}/v1/metadata"
        self.files_api = f"{host}/v1/files"

    def browse(self, path):
        """Browse files and directories.

        :param path: File/directory path
        """
        response = self.session.get(
            "{}/{}".format(self.files_api, path.strip('/'))
        )
        response.raise_for_status()
        return response.json()

    def _wait_response(self, response):
        status = "pending"
        polling_url = response.json()["polling_url"]

        while status == "pending":
            sleep(5)
            print('.', end='', flush=True)
            response = self.session.get(polling_url)
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

    def directory_files(self, target_directory):
        """Fetch file metadata for all files in directory."""
        # Get list of all files pre-ingest file storage
        directory_tree = self.session.get(self.files_api).json()
        all_file_paths = list()
        for directory, files in directory_tree.items():
            for file_ in files:
                all_file_paths.append(os.path.join(directory, file_))

        # List only files in target directory (and subdirectories)
        directory_file_paths = [path for path in all_file_paths
                                if path.startswith(target_directory)]

        # Get file metadata for files in directory (and subdirectories)
        files = list()
        for file_path in directory_file_paths:
            file_ = self.session.get(
                "{}/{}".format(self.files_api, file_path.strip('/'))
            ).json()
            parent_directory \
                = self.session.get(
                    "{}/{}".format(self.files_api,
                                   os.path.dirname(file_path).strip('/'))
                ).json()
            files.append({
                "parent_directory_identifier": parent_directory["identifier"],
                "identifier": file_["metax_identifier"],
                "checksum": file_["md5"],
                "path": file_["file_path"]
            })

        return files

    def upload_archive(self, source, target):
        """Upload archive to pre-ingest file storage.

        :param source: path to archive on local disk
        :param target: target directory path in pre-ingest file storage
        """
        # Check that the provided file is either a zip or tar archive
        if not tarfile.is_tarfile(source) and not zipfile.is_zipfile(source):
            raise ValueError(f"Unsupported file: '{source}'")

        # Upload the package
        with open(source, "rb") as upload_file:
            response = self.session.post(
                self.archives_api,
                params={'dir': target.strip('/')},
                data=upload_file,
                headers={'Content-Md5': _md5_digest(source)}
            )

            # Print InsecureRequestWarning only once
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            try:
                response.raise_for_status()
            except HTTPError:
                if response.headers["content-type"] == "application/json":
                    print(json.dumps(response.json(), indent=4))
                    sys.exit(1)
                raise

        if response.status_code == 202:
            response = self._wait_response(response)

    def generate_directory_metadata(self, target):
        """Generate metadata for directory.

        :param source: path to archive on local disk
        :param target: target directory path in pre-ingest file storage
        """
        response = self.session.post(
            # Character "*" is added to url to enable metadata
            # generation for root directory. See TPASPKT-719 for more
            # information.
            "{}/{}*".format(self.metadata_api, target.strip('/'))
        )
        try:
            response.raise_for_status()
        except HTTPError:
            if response.headers["content-type"] == "application/json":
                print(json.dumps(response.json(), indent=4))
                sys.exit(1)
            raise
        if response.status_code == 202:
            response = self._wait_response(response)
        return self.session.get("{}/{}".format(self.files_api,
                                               target.strip('/'))).json()


def main(cli_args=None):
    """Parse command line arguments and run the chosen function.

    :param cli_args: command line arguments
    """
    args = _parse_args(cli_args)

    verify = not args.insecure
    host, user, password = _parse_conf_file(args.config)

    client = PreIngestFileStorage(verify, host, user, password)
    args.func(client, args)


if __name__ == "__main__":
    main(sys.argv[1:])
