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

upload-rest-api-client requires you to either specify a project identifier when
using commands or define a default project in the :code:`~/.upload.cfg`
configuration file.

You can check a list of accessible projects and their corresponding quotas by
running::

    upload-client list-projects

upload-rest-api-client can be used to send tar or zip archives. If you want to
upload package :code:`package.zip`, simply run::

    upload-client upload --project <project> package.zip


Copyright
~~~~~~~~~
Copyright (C) 2019 CSC - IT Center for Science Ltd.

This program is free software: you can redistribute it and/or modify it under the terms
of the GNU Lesser General Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with
this program.  If not, see <https://www.gnu.org/licenses/>.
