# -*- coding: utf-8 -*-

from preserve.connector import Connector
from preserve.preserve import Preserve, connectors, from_uri, open


__all__ = ["Connector", "Preserve", "connectors", "from_uri", "open"]


def __get_pyproject():
    import io
    import os

    import toml

    init_path = os.path.abspath(os.path.dirname(__file__))
    pyproject_path = os.path.join(init_path, "../pyproject.toml")

    with io.open(pyproject_path, "r") as fopen:
        pyproject = toml.load(fopen)

    return pyproject["tool"]["poetry"]


__author__ = "Grégoire Burel"
__copyright__ = "Grégoire Burel"
__license__ = "MIT"

__version__ = __get_pyproject()["version"]
__doc__ = __get_pyproject()["description"]
