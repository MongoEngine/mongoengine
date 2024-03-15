import threading
from typing import Optional

from pymongo.client_session import ClientSession

_sessions = threading.local()


def set_local_session(db_alias: str, session: ClientSession):
    _sessions.__setattr__(key_local_session(db_alias), session)


def get_local_session(db_alias: Optional[str] = None) -> Optional[ClientSession]:
    if db_alias is None:
        from mongoengine.connection import DEFAULT_CONNECTION_NAME

        db_alias = DEFAULT_CONNECTION_NAME
    try:
        return _sessions.__getattribute__(key_local_session(db_alias))
    except AttributeError:
        return None


def clear_local_session(db_alias: Optional[str] = None):
    if db_alias is None:
        from mongoengine.connection import DEFAULT_CONNECTION_NAME

        db_alias = DEFAULT_CONNECTION_NAME
    _sessions.__delattr__(key_local_session(db_alias))


def clear_all():
    global _sessions
    _sessions = threading.local()


def key_local_session(db_alias):
    return f"tomgoengine_session_{db_alias}"
