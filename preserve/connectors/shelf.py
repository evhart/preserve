from fileinput import filename
from shelve import DbfilenameShelf, Shelf

from typing import Optional

from weakref import WeakValueDictionary

from preserve.preserve import Connector
import os

from urllib import parse


class Shelf(Connector):
    """
    Preserve connector using Shelf backend.
    """

    filename: str
    protocol: Optional[str] = None
    writeback: bool = False
    keyencoding: str = "utf-8"

    __slots__ = ["_shelf"]

    @staticmethod
    def scheme() -> str:
        return "shelf"

    # example: shelf://filename?protocol=?,writeback=?, keyencoding="utf-8"
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._shelf = DbfilenameShelf(
            self.filename, flag="c", protocol=self.protocol, writeback=self.writeback
        )

    @classmethod
    def from_uri(cls, uri: str) -> "Shelf":
        p = parse.urlsplit(uri)
        if p.scheme != cls.scheme():
            raise ValueError()

        params = {}
        params["filename"] = p.path
        params.update(dict(parse.parse_qsl(p.query)))

        return cls.parse_obj(params)

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


# class MultiShelf(Connector):
#     @staticmethod
#     def scheme() -> str:
#         return "multi-shelf"

#     def __init__(self, filename, protocol=None, writeback=False, keyencoding="utf-8"):
#         super().__init__()
#         self.filename = filename
#         self.protocol = protocol
#         self.writeback = writeback
#         self.keyencoding = keyencoding

#         self.base_path = self.filename

#         if os.path.exists(self.base_path):
#             if not os.path.isfile(os.path.join(self.base_path, "_default_")):
#                 raise FileExistsError()
#         else:
#             os.makedirs(self.base_path)

#         self.multi = WeakValueDictionary()

#         self.multi["_default_".encode(self.keyencoding)] = ShelfConnector(
#             os.path.join(filename, "_default_"),
#             protocol=None,
#             writeback=False,
#             keyencoding="utf-8",
#         )

#     def __iter__(self):
#         return self.multi.__iter__()

#     def __len__(self):
#         return sum([len(i) for i in self.multi.values()])

#     def __contains__(self, key):
#         return self.multi.__contains__(key)

#     def get(self, key=None, default=None):
#         if not key:
#             return self.multi.get("_default_", default)
#         if key not in self.multi:
#             filename = os.path.join(self.base_path, key)
#             s = ShelfConnector(
#                 filename,
#                 protocol=self.protocol,
#                 writeback=self.writeback,
#                 keyencoding=self.keyencoding,
#             )
#             self.multi[key.encode(self.keyencoding)] = s
#             return s
#         else:
#             return self.multi.get(key.encode(self.keyencoding), default)

#     def __getitem__(self, key=None):
#         return self.get(key)

#     def __setitem__(self, key, value):
#         self.multi["_default_".encode(self.keyencoding)].__setitem__(key, value)

#     def __delitem__(self, key=None):
#         if not key:
#             self.multi.__delitem__("_default_")
#         self.multi.__delitem__(key)

#     def __enter__(self):
#         return self

#     def __exit__(self, type, value, traceback):
#         self.close()

#     def close(self):
#         for i in self.multi.values():
#             i.close()

#     def __del__(self):
#         self.close()

#     def sync(self):
#         for i in self.multi.values():
#             i.sync()
