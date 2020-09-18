# from urllib.parse import urlparse


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


# def uri_mapper(
#     uri: str,
#     netloc: str = "netloc",
#     first_path: str = "netloc",
#     second_path: str = "netloc",
# ) -> dict[str, str]:
#     """
#     Maps a URI to a dictionarry that can be used for generating connector inputs.
#     Fragments/scheme are ignored.
#     Only the first parts of th path are taken (support for database/collection)
#     Query elements are returned directly (flatten)
#     """
#     o = urlparse(uri)
