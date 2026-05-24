class GridFSError(Exception):
    pass


class ImproperlyConfigured(Exception):
    pass


__all__ = ("GridFSError", "ImproperlyConfigured")
