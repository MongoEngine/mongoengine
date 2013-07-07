__all__ = ["blacklist", "whitelist", "wholelist"]


def blacklist(*fields):
    """A blacklist is a list of fields explicitly named that are not allowed.
    """
    def predicator(k, v):
        if fields:
            return k in fields

        return False

    return predicator


def whitelist(*fields):
    """A whitelist is a list of fields explicitly named that are allowed.
    """
    def predicator(k, v):
        if fields:
            return k not in fields

        return True

    return predicator


def wholelist(*fields):
    """A wholelist is a role that allows all fields.
    """
    def predicator(k, v):
        return False

    return predicator
