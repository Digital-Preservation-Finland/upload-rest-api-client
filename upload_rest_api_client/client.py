"""Upload-rest-api-client CLI."""
from __future__ import print_function

import argparse
import configparser
import os
import sys

import argcomplete
from tabulate import tabulate

from upload_rest_api_client.pre_ingest_file_storage import PreIngestFileStorage


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
        "project",
        help="project to browse"
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
        "project",
        help="project to upload files to"
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


def _list_projects(client, args):
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
    resource = client.browse(args.project, args.path)
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
    client.upload_archive(
        project=args.project,
        source=args.source,
        target=target
    )
    print(f"Uploaded '{args.source}'")

    # Generate metadata
    directory = client.generate_directory_metadata(
        project=args.project,
        target=target
    )

    if args.output:
        files = client.directory_files(args.project, target)
        with open(args.output, "w") as f_out:
            for file_ in files:
                f_out.write("{}\t{}\t{}\t{}\n".format(*file_.values()))

    # Print path and identifier of uploaded directory. Root directory
    # identifier is NOT printed to avoid root directory accidentally
    # being included in a dataset.
    message = f"Generated metadata for directory: {target}"
    if target != "/":
        message += " (identifier: {})".format(directory['identifier'])
    print(message)

    if directory['directories']:
        # Print list of subdirectories
        print("\nThe directory contains subdirectories:")
        for subdirectory in directory['directories']:
            identifier = client.browse(
                args.project, f"{target}/{subdirectory}"
            )["identifier"]
            print(f"{subdirectory} (identifier: {identifier})")


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
