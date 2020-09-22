import shelve
from typing import Optional, cast
from urllib import parse

from preserve.connector import Connector


class Shelf(Connector):
    """Preserve connector using Shelf backend."""

    filename: str
    protocol: Optional[str] = None
    writeback: bool = False
    keyencoding: str = "utf-8"

    __slots__ = ["_shelf"]

    def __setattr__(self, attr, value):
        if attr in self.__slots__:
            object.__setattr__(self, attr, value)
        else:
            super().__setattr__(attr, value)

    @staticmethod
    def scheme() -> str:
        return "shelf"

    # example: shelf://filename?protocol=?,writeback=?, keyencoding="utf-8"
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._shelf = shelve.DbfilenameShelf(
            self.filename,
            flag="c",
            protocol=self.protocol,
            writeback=self.writeback,
        )

    @classmethod
    def from_uri(cls, uri: str) -> "Shelf":
        p = parse.urlsplit(uri)
        if p.scheme != cls.scheme():
            raise ValueError()

        params = {}
        params["filename"] = p.path
        params.update(dict(parse.parse_qsl(p.query)))

        return cast("Shelf", cls.parse_obj(params))

    def __iter__(self):
        return self._shelf.__iter__()

    def __len__(self):
        return self._shelf.__len__()

    def __contains__(self, key):
        return self._shelf.__contains__(key)

    def get(self, key, default=None):
        return self._shelf.get(key, default=default)

    def __getitem__(self, key):
        return self._shelf.__getitem__(key)

    def __setitem__(self, key, value):
        self._shelf.__setitem__(key, value)

    def __delitem__(self, key):
        self._shelf.__delitem__(key)

    def __enter__(self):
        return self._shelf

    def __exit__(self, type, value, traceback):
        self._shelf.close()

    def close(self):
        self._shelf.close()

    def __del__(self):
        self.close()

    def sync(self):
        self._shelf.sync()
