"""Pre ingest file storage client module."""

import hashlib
import os
import tarfile
import warnings
import zipfile

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
import urllib3

from upload_rest_api_client import __version__


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


class HTTPBearerAuth(requests.auth.AuthBase):
    """
    Authorization class for Requests that implements Bearer authorization.

    This is used instead of changing headers directly to prevent
    requests from using the `.netrc` file if that is found on the
    system.
    """

    def __init__(self, token):
        self.token = token

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.token}"
        return request


class PreIngestFileNotFoundError(Exception):
    """Exception raised when a file cannot be found in the pre-ingest
    file storage.
    """
    pass


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

        # Do not retry requests to ensure that upload requests are not
        # sent multiple times.
        self.session.mount(host, requests.adapters.HTTPAdapter(max_retries=0))

        self.session.headers["User-Agent"] = (
            f"upload-rest-api-client/{__version__} "
            f"(github.com/Digital-Preservation-Finland/"
            f"upload-rest-api-client)"
        )

        # Automatically run 'raise_for_status' for each response
        def check_status(resp, **_):
            """Check status for each response."""
            resp.raise_for_status()
        self.session.hooks["response"] = [check_status]

        self.archives_api = f"{host}/v1/archives"
        self.metadata_api = f"{host}/v1/metadata"
        self.files_api = f"{host}/v1/files"
        self.users_api = f"{host}/v1/users"
        self.tasks_api = f"{host}/v1/tasks"

        if config.get("default_project"):
            self.default_project = config["default_project"]
        else:
            self.default_project = None

        self._configure_auth(config)

    def _configure_auth(self, config):
        """
        Configure authentication, using either a token or an
        username + password combination, preferring token if available.
        """
        if config["token"]:
            self.session.auth = HTTPBearerAuth(config["token"])
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
        """Retrieve dictionary of projects accessible to the user."""
        response = self.session.get(f"{self.users_api}/projects")
        return response.json()["projects"]

    def browse(self, project, path):
        """Browse files and directories.

        :param project: Project identifier
        :param path: File/directory path
        :raises: PreIngestFileNotFoundError when a browsed file
            is not found in the pre-ingest file storage
        :raises: HTTPError for HTTP errors
        """
        try:
            response = self.session.get(
                f"{self.files_api}/{project}/{path.strip('/')}"
            )
        except HTTPError as exc:
            # Catch the error of trying to browse a file that does not
            # exist, separating it from page not found errors. If
            # response is JSON, we assume it comes from upload-rest-api
            is_json = exc.response.headers["content-type"] == (
                "application/json")
            if exc.response.status_code == 404 and is_json:
                raise PreIngestFileNotFoundError(
                    exc.response.json()["error"]
                ) from exc
            raise

        return response.json()

    def task_status(self, task_id):
        """Check task status.

        :param task_id: task identifier
        """
        response = self.session.get(f"{self.tasks_api}/{task_id}")
        task = response.json()
        task['identifier'] = task_id
        return task

    def directory_files(self, project, target_directory):
        """Fetch file metadata for all files in directory."""
        # Get tree of all files pre-ingest file storage
        directory_tree = self.session.get(
            f"{self.files_api}/{project}",
            params={"all": "true"}
        ).json()
        all_file_paths = []
        for directory, files in directory_tree.items():
            for file_ in files:
                all_file_paths.append(os.path.join(directory, file_))

        # List only files in target directory (and subdirectories)
        directory_file_paths = [path for path in all_file_paths
                                if path.startswith(target_directory)]

        # Get file metadata for files in directory (and subdirectories)
        files = []
        for file_path in directory_file_paths:
            file_ = self.session.get(
                f"{self.files_api}/{project}/{file_path.strip('/')}"
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

        if response.status_code == 202:
            task_id = response.json()['polling_url'].strip("/").split("/")[-1]
            return {'status': 'pending', 'identifier': task_id}
        return {'status': 'done', 'identifier': None}

    def delete(self, project, path):
        """Delete a file or directory from pre-ingest file storage.

        Deletes also the metadata of the files.

        :param project: target project ID
        :param path: path in pre-ingest file storage to the file or
                     directory
                     that is to be deleted.
        """
        path = path.strip("/")
        response = self.session.delete(
            f"{self.files_api}/{project}/{path}"
        )

        # When deleting directories the metadata of the files is deleted
        # in a pollable task. Wait for the task to finish.
        if response.status_code == 202:
            task_id = response.json()['polling_url'].strip("/").split("/")[-1]
            return {'status': 'pending', 'identifier': task_id}
        return {'status': 'done', 'identifier': None}
