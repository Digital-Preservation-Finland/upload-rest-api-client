"""Pre ingest file storage client module."""
from __future__ import print_function

import hashlib
import json
import os
import sys
import tarfile
import warnings
import zipfile
from time import sleep

import requests
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


class PreIngestFileStorage():
    """Pre-ingest file storage client."""

    def __init__(self, verify, config):
        """Initialize pre-ingest file storage client.

        :param bool verify: Use SSL verification
        :param dict config: Configuration dict containing the fields
                            `host`, `user`, `password` and `token`
        """
        host = config["host"]

        self.session = requests.Session()
        self.session.verify = verify
        self.session.mount(host, requests.adapters.HTTPAdapter(max_retries=5))
        self.archives_api = f"{host}/v1/archives"
        self.metadata_api = f"{host}/v1/metadata"
        self.files_api = f"{host}/v1/files"
        self.users_api = f"{host}/v1/users"

        self._configure_auth(config)

    def _configure_auth(self, config):
        """
        Configure authentication, using either a token or an
        username + password combination, preferring token if available.
        """
        if config["token"]:
            self.session.headers["Authorization"] = f"Bearer {config['token']}"
        else:
            self.session.auth = HTTPBasicAuth(
                config["user"], config["password"]
            )
            warnings.warn(
                "User + password authentication is deprecated. Please "
                "create a token using the Fairdata Digital Preservation "
                "System web UI and set it to 'upload/token' field in the "
                "configuration file.",
                UserWarning
            )

    def get_projects(self):
        """Retrieve dictionary of projects accessible to the user"""
        response = self.session.get(f"{self.users_api}/projects")
        response.raise_for_status()
        return response.json()["projects"]

    def browse(self, project, path):
        """Browse files and directories.

        :param project: Project identifier
        :param path: File/directory path
        """
        response = self.session.get(
            "{}/{}/{}".format(self.files_api, project, path.strip('/'))
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

    def directory_files(self, project, target_directory):
        """Fetch file metadata for all files in directory."""
        # Get tree of all files pre-ingest file storage
        directory_tree = self.session.get(
            f"{self.files_api}/{project}",
            params={"all": "true"}
        ).json()
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
                "{}/{}/{}".format(
                    self.files_api, project, file_path.strip('/')
                )
            ).json()
            parent_directory \
                = self.session.get(
                    "{}/{}/{}".format(
                        self.files_api,
                        project,
                        os.path.dirname(file_path).strip('/')
                    )
                ).json()
            files.append({
                "parent_directory_identifier": parent_directory["identifier"],
                "identifier": file_["identifier"],
                "checksum": file_["md5"],
                "path": file_["file_path"]
            })

        return files

    def upload_archive(self, project, source, target):
        """Upload archive to pre-ingest file storage.

        :param project: target project ID
        :param source: path to archive on local disk
        :param target: target directory path in pre-ingest file storage
        """
        # Check that the provided file is either a zip or tar archive
        if not tarfile.is_tarfile(source) and not zipfile.is_zipfile(source):
            raise ValueError(f"Unsupported file: '{source}'")

        # Upload the package
        with open(source, "rb") as upload_file:
            response = self.session.post(
                f"{self.archives_api}/{project}",
                params={'dir': target.strip('/'), 'md5': _md5_digest(source)},
                data=upload_file,
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

    def generate_directory_metadata(self, project, target):
        """Generate metadata for directory.

        :param project: target project ID
        :param target: target directory path in pre-ingest file storage
        """
        response = self.session.post(
            # Character "*" is added to url to enable metadata
            # generation for root directory. See TPASPKT-719 for more
            # information.
            "{}/{}/{}*".format(
                self.metadata_api, project, target.strip('/')
            )
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
        return self.session.get(
            "{}/{}/{}".format(self.files_api, project, target.strip('/'))
        ).json()
