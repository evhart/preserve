from platformdirs import user_data_path
from usingversion import getattr_with_version

from preserve.cache import Cache, cache
from preserve.connector import Connector, MultiConnector
from preserve.preserve import (
    Preserve,
    connectors,
    from_uri,
    from_uri_multi,
    multi_connectors,
    open,
    open_multi,
)

__all__ = [
    "Cache",
    "cache",
    "Connector",
    "MultiConnector",
    "Preserve",
    "connectors",
    "from_uri",
    "from_uri_multi",
    "multi_connectors",
    "open",
    "open_multi",
]

APPNAME = "preserve"
APP_REPOSITORY = "https://github.com/evhart/preserve"
DEFAULT_ROOT_PATH = user_data_path(appname=APPNAME)

__getattr__ = getattr_with_version(APPNAME, __file__, __name__)
