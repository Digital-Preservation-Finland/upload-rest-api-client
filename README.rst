upload-rest-api-client
======================

Installation
~~~~~~~~~~~~

Make sure you have Python 3 installed::

    yum install -y python3

Install upload-rest-api-client in a virtual environment::

    make
    source venv/bin/activate

Add your credentials to the created configuration file :code:`~/.upload.cfg`
with the editor editor of your choice, e.g.::

    vi ~/.upload.cfg

Usage
~~~~~

upload-rest-api-client can be used to send tar or zip archives. If you want to
upload package :code:`package.zip`, simply run::

    upload package.zip
