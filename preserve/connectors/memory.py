import shelve
from typing import cast
from urllib import parse

from preserve.connector import Connector


class Memory(Connector):
    """Memory-based preserve connector."""

    keyencoding: str = "utf-8"

    __slots__ = "_dict"

    @staticmethod
    def scheme() -> str:
        return "memory"

    @classmethod
    def from_uri(cls, uri: str) -> "Memory":
        p = parse.urlsplit(uri)
        if p.scheme != cls.scheme():
            raise ValueError()

        return cast("Memory", cls.parse_obj(dict(parse.parse_qsl(p.query))))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dict = {}

    def __iter__(self):
        for k in self._dict.keys():
            yield k.decode(self.keyencoding)

    def __len__(self):
        return len(self._dict)

    def __contains__(self, key):
        return key.encode(self.keyencoding) in self._dict

    def get(self, key, default=None):
        if key.encode(self.keyencoding) in self._dict:
            return self._dict[key.encode(self.keyencoding)]
        return default

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self._dict[key.encode(self.keyencoding)] = value

    def __delitem__(self, key):
        try:
            del self._dict[key]
        except KeyError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self._dict is None:
            return
        try:
            self.sync()
            try:
                self._dict.close()
            except AttributeError:
                pass
        finally:
            # Catch errors that may happen when close is called from __del__
            # because CPython is in interpreter shutdown.
            try:
                self._dict = shelve._ClosedDict()
            except ValueError:
                self._dict = None

    def __del__(self):
        self.close()

    def sync(self):
        pass
