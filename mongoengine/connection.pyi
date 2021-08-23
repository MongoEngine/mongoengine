from typing import Dict, Union

from pymongo import MongoClient, database

_connections: Dict[str, Union[MongoClient, object]]

__all__ = ["_connections"]

DEFAULT_CONNECTION_NAME: str

def get_db(alias=DEFAULT_CONNECTION_NAME, reconnect=False) -> database.Database: ...

def get_connection(alias=DEFAULT_CONNECTION_NAME, reconnect=False) -> MongoClient: ...

_get_connection = get_connection
_get_db = get_db
