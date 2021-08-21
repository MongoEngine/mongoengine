from typing import Optional

from pymongo.database import Database

class MongoClient:
    def __init__(
        self,
        host: Optional[str] = ...,
        port: int = ...,
        document_class: object = ...,
        tz_aware: Optional[object] = ...,
        connect: Optional[object] = ...,
        type_registry: Optional[object] = ...,
        **kwargs: object
    ) -> None: ...
    def __getitem__(self, key: str) -> Database: ...
    def __getattr__(self, key: str) -> Database: ...
    def get_database(self) -> Database: ...
    def get_default_database(self) -> Database: ...
    admin: Database
