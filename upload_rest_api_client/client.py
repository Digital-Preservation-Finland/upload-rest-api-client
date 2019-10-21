"""upload-rest-api-client"""
from __future__ import print_function

import os
import configparser


def _parse_conf_file():
    """Parse configuration from ~/.upload.cfg
    
    :returns: Parsed configuration
    """
    configuration = configparser.ConfigParser()
    configuration.read(os.path.expanduser("~/.upload.cfg"))
    return configuration


def main():
    conf = _parse_conf_file()
    print(conf.get("upload", "host"))
    print(conf.get("upload", "user"))
    print(conf.get("upload", "password"))
