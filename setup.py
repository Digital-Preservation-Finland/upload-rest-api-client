from setuptools import setup, find_packages


def main():
    """Install upload-rest-api"""
    setup(
        name='upload-rest-api-client',
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        setup_requires=["setuptools-scm"],
        use_scm_version=True,
        install_requires=[
            "requests",
            "argcomplete",
            "tabulate",
        ],
        entry_points={
            "console_scripts": [
                "upload-client = upload_rest_api_client.client:main"
            ]
        }
    )


if __name__ == '__main__':
    main()
