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
        "target",
        help="directory where the uploaded archive is extracted"
    )
    # TODO: In Python 3.8 optional bool arguments can be implemented
    # using argparse.BooleanOptionalAction class
    upload_parser.add_argument(
        "--gtk",
        default=True,
        action='store_true',
        help="Print information about subdirectories and files when "
             "uploading archives."
    )
    upload_parser.add_argument(
        "--no-gtk",
        dest='gtk',
        action='store_false',
        help="Simple output format."
    )
    upload_parser.set_defaults(func=_upload)

    # Setup bash auto completion
    argcomplete.autocomplete(parser)

    # Parse arguments
    args = parser.parse_args()
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
        print("{}:".format(key))
        value_list = value if isinstance(value, list) else [value]
        for value in value_list:
            print("    {}".format(value))
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
    print("Uploaded '%s'" % args.source)

    # Generate metadata
    directory = client.generate_directory_metadata(target)

    # Print information about about generated metadata
    if args.gtk:
        files = client.directory_files(target)

        print("Generated file metadata\n")
        print_format = "%45s    %45s    %32s    %s"
        print(print_format % (
            "parent_dir",
            "identifier",
            "checksum_value",
            "file_path"
        ))
        for file_ in files:
            print(print_format % tuple(file_.values()))

        if args.output:
            with open(args.output, "w") as f_out:
                for file_ in files:
                    f_out.write("%s\t%s\t%s\t%s\n" % tuple(file_.values()))

    else:
        print("Generated metadata for directory: /{}\n"
              "Directory identifier: {}".format(target,
                                                directory['identifier']))


class PreIngestFileStorage():
    """Pre-ingest file storage client."""

    def __init__(self, verify, host, user, password):
        """Initialize pre-ingest file storage client.

        :param bool verify: Use SSL verification
        :param host: API url
        :param user: username
        :param password: password
        """
        self.verify = verify
        self.host = host
        self.user = user
        self.auth = HTTPBasicAuth(user, password)
        self.archives_api = "%s/v1/archives" % host
        self.metadata_api = "%s/v1/metadata" % host
        self.files_api = "%s/v1/files" % host

    def browse(self, path):
        """Browse files and directories.

        :param path: File/directory path
        """
        response = requests.get("%s/%s" % (self.files_api, path.strip('/')),
                                auth=self.auth,
                                verify=self.verify)
        response.raise_for_status()
        return response.json()

    def _wait_response(self, response):
        status = "pending"
        polling_url = response.json()["polling_url"]

        while status == "pending":
            sleep(5)
            print('.', end='', flush=True)
            response = requests.get(polling_url,
                                    auth=self.auth,
                                    verify=self.verify)
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
        directory_tree = requests.get(self.files_api,
                                      auth=self.auth,
                                      verify=self.verify).json()
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
            file_ = requests.get("{}/{}".format(self.files_api, file_path),
                                 auth=self.auth,
                                 verify=self.verify).json()
            parent_directory \
                = requests.get("{}/{}".format(self.files_api,
                                              os.path.dirname(file_path)),
                               auth=self.auth,
                               verify=self.verify).json()
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
            raise ValueError("Unsupported file: '%s'" % source)

        # Upload the package
        with open(source, "rb") as upload_file:
            response = requests.post(
                "%s?dir=%s" % (self.archives_api, target.strip('/')),
                data=upload_file,
                auth=self.auth,
                verify=self.verify
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
        response = requests.post(
            "%s/%s/" % (self.metadata_api, target.strip('/')),
            auth=self.auth,
            verify=self.verify
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
        return requests.get("%s/%s/" % (self.files_api,
                                        target.strip('/'))).json()


def main():
    """Parse command line arguments and run the chosen function."""
    args = _parse_args()

    # Check that the configuration file exists
    if not os.path.isfile(os.path.expanduser(args.config)):
        raise ValueError("Config file '%s' not found" % args.config)

    verify = not args.insecure
    host, user, password = _parse_conf_file(args.config)

    client = PreIngestFileStorage(verify, host, user, password)
    args.func(client, args)


if __name__ == "__main__":
    main()
