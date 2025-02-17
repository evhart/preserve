from platformdirs import user_data_path
from usingversion import getattr_with_version

from preserve.connector import Connector
from preserve.preserve import Preserve, connectors, from_uri, open

__all__ = ["Connector", "Preserve", "connectors", "from_uri", "open"]

APPNAME = "preserve"
APP_REPOSITORY = "https://github.com/evhart/preserve"
DEFAULT_ROOT_PATH = user_data_path(appname=APPNAME)

__getattr__ = getattr_with_version(APPNAME, __file__, __name__)
