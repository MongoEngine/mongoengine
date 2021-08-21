from typing import Tuple

import pymongo.cursor as cursor
import pymongo.database as database
from pymongo import aggregation as aggregation
from pymongo import auth as auth
from pymongo import auth_aws as auth_aws
from pymongo import bulk as bulk
from pymongo import change_stream
from pymongo import client_options as client_options
from pymongo import client_session as client_session
from pymongo import collation as collation
from pymongo import collection as collection
from pymongo import command_cursor as command_cursor
from pymongo import common as common
from pymongo import compression_support as compression_support
from pymongo import cursor_manager as cursor_manager
from pymongo import driver_info as driver_info
from pymongo import encryption_options as encryption_options
from pymongo import errors
from pymongo import helpers as helpers
from pymongo import ismaster as ismaster
from pymongo import max_staleness_selectors as max_staleness_selectors
from pymongo import message as message
from pymongo import mongo_client as mongo_client
from pymongo import mongo_replica_set_client as mongo_replica_set_client
from pymongo import monitor as monitor
from pymongo import monitoring as monitoring
from pymongo import monotonic as monotonic
from pymongo import network as network
from pymongo import operations as operations
from pymongo import periodic_executor as periodic_executor
from pymongo import pool as pool
from pymongo import read_concern as read_concern
from pymongo import read_preferences as read_preferences
from pymongo import response as response
from pymongo import results as results
from pymongo import saslprep as saslprep
from pymongo import server as server
from pymongo import server_description as server_description
from pymongo import server_selectors as server_selectors
from pymongo import server_type as server_type
from pymongo import settings as settings
from pymongo import socket_checker as socket_checker
from pymongo import son_manipulator as son_manipulator
from pymongo import srv_resolver as srv_resolver
from pymongo import ssl_context as ssl_context
from pymongo import ssl_match_hostname as ssl_match_hostname
from pymongo import ssl_support as ssl_support
from pymongo import thread_util as thread_util
from pymongo import topology as topology
from pymongo import topology_description as topology_description
from pymongo import uri_parser as uri_parser
from pymongo import write_concern as write_concern
from pymongo.collection import ReturnDocument as ReturnDocument
from pymongo.common import MAX_SUPPORTED_WIRE_VERSION as MAX_SUPPORTED_WIRE_VERSION
from pymongo.common import MIN_SUPPORTED_WIRE_VERSION as MIN_SUPPORTED_WIRE_VERSION
from pymongo.cursor import CursorType as CursorType
from pymongo.mongo_client import MongoClient as MongoClient
from pymongo.mongo_replica_set_client import (
    MongoReplicaSetClient as MongoReplicaSetClient,
)
from pymongo.operations import DeleteMany as DeleteMany
from pymongo.operations import DeleteOne as DeleteOne
from pymongo.operations import IndexModel as IndexModel
from pymongo.operations import InsertOne as InsertOne
from pymongo.operations import ReplaceOne as ReplaceOne
from pymongo.operations import UpdateMany as UpdateMany
from pymongo.operations import UpdateOne as UpdateOne
from pymongo.read_preferences import ReadPreference as ReadPreference
from pymongo.write_concern import WriteConcern as WriteConcern
from typing_extensions import Literal

ASCENDING: Literal[1] = 1
DESCENDING: Literal[-1] = -1
GEO2D: Literal["2d"] = "2d"
GEOHAYSTACK: Literal["geoHaystack"] = "geoHaystack"
GEOSPHERE: Literal["2dsphere"] = "2dsphere"
HASHED: Literal["hashed"] = "hashed"
TEXT: Literal["text"] = "text"
OFF: Literal[0] = 0
SLOW_ONLY: Literal[1] = 1
ALL: Literal[2] = 2

version_tuple: Tuple[int, int, int]

version: str

def get_version_string() -> str: ...
def has_c() -> bool: ...

__all__ = [
    "ASCENDING",
    "DESCENDING",
    "GEO2D",
    "GEOHAYSTACK",
    "GEOSPHERE",
    "HASHED",
    "TEXT",
    "get_version_string",
    "has_c",
    "MongoClient",
    "ReadPreference",
    "errors",
    "cursor",
    "change_stream",
]
