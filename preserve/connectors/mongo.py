from weakref import WeakValueDictionary

from bson.objectid import ObjectId
from pymongo import MongoClient

from typing import Optional

from preserve.preserve import Connector
from urllib import parse


class Mongo(Connector):
    """
    Mongodb backend for preserve.
    """

    database: str = "db"
    collection: Optional[str] = None
    host: str = "127.0.0.1"
    port: int = 27017

    __slots__ = ["_client", "_collection"]

    @classmethod
    def from_uri(cls, uri: str) -> "Shelf":
        p = parse.urlsplit(uri)
        if p.scheme != cls.scheme():
            raise ValueError()

        params = {}
        if p.hostname:
            params["host"] = p.hostname

        if p.port:
            params["port"] = p.port

        if p.path and len(p.path.split("/", 1)) > 1:
            params["database"] = p.path.split("/", 1)[1]

        params.update(dict(parse.parse_qsl(p.query)))

        print(params)
        return cls.parse_obj(params)

    @staticmethod
    def scheme() -> str:
        return "mongodb"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not self.collection:
            self.collection = self.database

        self._client = MongoClient(self.host, self.port)
        self._collection = self._client[self.database][self.collection]

    def __iter__(self):
        for item in self._collection.find():
            yield item["_id"]

    def items(self):
        for item in self._collection.find():
            _id = item["_id"]
            del item["_id"]
            yield _id, item

    def __len__(self):
        return self._collection.count_documents({})

    def __contains__(self, key):
        if (
            self._collection.count_documents(
                {"_id": key.encode(self.keyencoding)}, limit=1
            )
            != 0
        ):
            return True
        else:
            return False

    def get(self, key, default=None):
        item = self.__getitem__(key)
        return item if item else default

    def __getitem__(self, key):
        item = self._collection.find_one({"_id": key})
        if item:
            del item["_id"]
        return item

    def __setitem__(self, key, value):
        item = dict(value)
        item["_id"] = key
        self._collection.update_one({"_id": key}, {"$set": item}, upsert=True)

    def __delitem__(self, key):
        self._collection.delete_one({"_id": key})

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._client.close()

    def close(self):
        self._client.close()

    def __del__(self):
        self.close()

    def sync(self):
        pass


# class MultiMongo(Connector):
#     @staticmethod
#     def scheme() -> str:
#         return "multi-mongodb"

#     def __init__(self, database: str = "db", host="127.0.0.1", port=27017):
#         super().__init__()
#         self.host = host
#         self.port = port
#         self.database = database

#         self.client = MongoClient(self.host, self.port)
#         self._database = self.client[self.database]

#     def __iter__(self):
#         for collection in self._database.list_collection_names():
#             yield self[collection]

#     def items(self):
#         for collection in self._database.list_collection_names():
#             yield collection, self[collection]

#     def __len__(self):
#         return sum([len(i) for i in self])

#     def __contains__(self, key):
#         return key in self._database.list_collection_names()

#     def get(self, key=None, default=None):
#         return self.__getitem__(key)

#     def __getitem__(self, key=None):
#         return Mongo(
#             database=self.database, collection=key, host=self.host, port=self.port
#         )

#     def __setitem__(self, key, value):
#         pass

#     def __delitem__(self, key=None):
#         self[key].drop()

#     def __enter__(self):
#         return self

#     def __exit__(self, type, value, traceback):
#         self.close()

#     def close(self):
#         self.client.close()

#     def __del__(self):
#         self.close()

#     def sync(self):
#         pass
