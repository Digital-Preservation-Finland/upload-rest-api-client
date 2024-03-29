"""Upload-rest-api-client CLI."""

import argparse
import configparser
import json
import os
import sys
from time import sleep

import argcomplete
from requests.exceptions import HTTPError
from tabulate import tabulate

from upload_rest_api_client import __version__
from upload_rest_api_client.pre_ingest_file_storage import (
    PreIngestFileStorage, PreIngestFileNotFoundError
)


class TaskError(Exception):
    """Exception raised when a task in pre-ingest file storage fails."""

    def __init__(self, task):
        """Initialize TaskError."""
        self.task = task


def _parse_conf_file(conf):
    """Parse configuration file.

    :param conf: Path to the configuration file
    :returns: host, username, password
    """
    if not os.path.isfile(os.path.expanduser(conf)):
        raise ValueError(f"Config file '{conf}' not found")

    configuration = configparser.ConfigParser()
    configuration.read(os.path.expanduser(conf))
    return {
        "host": configuration["upload"].get("host"),
        "user": configuration["upload"].get("user"),
        "password": configuration["upload"].get("password"),
        "token": configuration["upload"].get("token"),
        "default_project": configuration["upload"].get("default_project")
    }


def _get_project_name(args, client):
    """Return the project name from command-line arguments or the config
    object.

    If no project is provided as a CLI parameter, check for
    `default_project` in configuration file instead. If none are
    provided, print a warning.

    :param args: command line arguments
    :param client: pre-ingest file storage client
    """
    if args.project:
        return args.project

    if client.default_project:
        return client.default_project

    raise RuntimeError(
        "Project name was not provided!\n\n"
        "You can provide the project name using --project command-line option "
        "or by using the `upload/default_project` field in the configuration "
        "file."
    )


def _wait_response(client, task):
    while task['status'] == "pending":
        sleep(5)
        print('.', end='', flush=True)
        task = client.task_status(task['identifier'])

    if task['status'] == "error":
        # Task with an error is still a succesful request, meaning
        # that raise_for_status() does not pick it up. Raise custom
        # error for these situations.
        raise TaskError(task)

    return task


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
            "include host and token in section [upload]."
        )
    )
    parser.add_argument(
        "-V", "--version", action="version", version=__version__,
        help="show client version and exit"
    )
    parser.set_defaults(func=None)
    subparsers = parser.add_subparsers(title="command")

    # List projects
    list_projects_parser = subparsers.add_parser(
        "list-projects", help="list accessible projects"
    )
    list_projects_parser.set_defaults(func=_list_projects)

    # Browse parser
    browse_parser = subparsers.add_parser(
        "browse", help="browse files in pre-ingest file storage"
    )
    browse_parser.add_argument(
        "path",
        help="path to file or directory"
    )
    browse_parser.add_argument(
        "--project",
        help="project to browse",
        default=None
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
        "--project",
        help="project to upload files to",
        default=None
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

    # Delete parser
    delete_parser = subparsers.add_parser(
        "delete", help="delete files from pre-ingest file storage"
    )
    delete_parser.add_argument(
        "--project",
        help="project where files are deleted from",
        default=None
    )
    delete_parser.add_argument(
        "path",
        help="path to the file or directory that is deleted"
    )
    delete_parser.set_defaults(func=_delete)

    # Setup bash auto completion
    argcomplete.autocomplete(parser)

    # Parse arguments
    args = parser.parse_args(cli_args)
    if not args.func:
        parser.print_help()
        sys.exit(0)
    return args


def _list_projects(client, _args):
    """List projects accessible to the user using client.

    :param client: Pre-ingest file storage client
    :param args: Arguments
    """
    projects = client.get_projects()

    if not projects:
        print("No projects available")
        return

    data = [
        (project["identifier"], project["used_quota"], project["quota"])
        for project in projects
    ]

    print(tabulate(data, headers=("Project", "Used quota", "Quota")))


def _browse(client, args):
    """Browse pre-ingest storage using client.

    Parse command specific command-line arguments and use provided
    client to browse pre-ingest file storage.

    :param client: Pre-ingest file storage client
    :param args: Browsing arguments
    """
    project = _get_project_name(args=args, client=client)
    try:
        resource = client.browse(project, args.path)
    except PreIngestFileNotFoundError as error:
        print(error)
        return

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
    project = _get_project_name(args=args, client=client)

    # Ensure that target directory path starts with slash
    target = f"/{args.target.strip('/')}"

    # Upload archive
    task = client.upload_archive(
        project=project,
        source=args.source,
        target=target
    )
    print(f"Uploaded '{args.source}'")

    # Wait until upload has been processed
    _wait_response(client, task)

    if args.output:
        files = client.directory_files(project, target)
        with open(args.output, "w") as f_out:
            for file_ in files:
                f_out.write("{}\t{}\t{}\t{}\n".format(*file_.values()))

    # Print path and identifier of uploaded directory. Root directory
    # identifier is NOT printed to avoid root directory accidentally
    # being included in a dataset.
    directory = client.browse(project, target)
    message = f"Generated metadata for directory: {target}"
    if target != "/":
        message += f" (identifier: {directory['identifier']})"
    print(message)

    if directory['directories']:
        # Print list of subdirectories
        print("\nThe directory contains subdirectories:")
        for subdirectory in directory['directories']:
            identifier = client.browse(
                project, f"{target}/{subdirectory}"
            )["identifier"]
            print(f"{subdirectory} (identifier: {identifier})")


def _delete(client, args):
    """Delete resources and their metadata from pre-ingest file storage."""
    project = _get_project_name(args=args, client=client)
    task = client.delete(project, args.path)
    _wait_response(client, task)
    print(f"Deleted '{args.path}' and all associated metadata.")


def main(cli_args=None):
    """Parse command line arguments and run the chosen function.

    :param cli_args: command line arguments
    """
    args = _parse_args(cli_args)

    verify = not args.insecure
    config = _parse_conf_file(args.config)

    client = PreIngestFileStorage(verify, config)

    try:
        args.func(client, args)
    except HTTPError as exc:
        if exc.response.headers["content-type"] == "application/json":
            print(f"Error when performing request to {exc.request.url}:")
            print(json.dumps(exc.response.json(), indent=4))
            sys.exit(1)
        raise
    except TaskError as exc:
        print(f"Error when polling task {exc.task['identifier']}:")
        print(json.dumps(exc.task, indent=4))


if __name__ == "__main__":
    main(sys.argv[1:])
