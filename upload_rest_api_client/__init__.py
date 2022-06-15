"""Upload-rest-api-rest-api-client default imports"""

from pkg_resources import DistributionNotFound, get_distribution

# TODO: Use importlib.metadata to get the version value more efficiently at
# runtime once we have upgraded to Python 3.8 (see
# https://packaging.python.org/en/latest/guides/single-sourcing-package-version
# and https://pypi.org/project/setuptools-scm/ for more information)
try:
    __version__ = get_distribution("upload_rest_api_client").version
except DistributionNotFound:
    __version__ = "unknown"
