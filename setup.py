from setuptools import setup, find_packages

from version import get_version


def main():
    """Install upload-rest-api"""
    setup(
        name='upload-rest-api-client',
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        version=get_version(),
        install_requires=[
            "requests"
        ],
        entry_points={
            "console_scripts": [
                "upload = upload_rest_api_client.client:main"
            ]
        }
    )


if __name__ == '__main__':
    main()
