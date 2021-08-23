from typing import Any, Dict, List, Optional, Tuple, Union

from pymongo.collation import Collation

_Hint = Union[str, List[Tuple[str, int]]]

class InsertOne:
    def __init__(self, document: Any) -> None: ...

class DeleteOne:
    def __init__(
        self,
        filter: Dict[str, Any],
        collation: Optional[Collation] = ...,
        hint: Optional[_Hint] = ...,
    ) -> None: ...

class DeleteMany:
    def __init__(
        self,
        filter: Dict[str, Any],
        collation: Optional[Collation] = ...,
        hint: Optional[_Hint] = ...,
    ) -> None: ...

class ReplaceOne:
    def __init__(
        self,
        filter: Dict[str, Any],
        replacement: Any,
        upsert: bool = ...,
        collation: Optional[Collation] = ...,
        hint: Optional[_Hint] = ...,
    ) -> None: ...

class UpdateOne:
    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = ...,
        collation: Optional[Collation] = ...,
        array_filters: List[Dict[str, Any]] = ...,
        hint: Optional[_Hint] = ...,
    ) -> None: ...

class UpdateMany:
    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = ...,
        collation: Optional[Collation] = ...,
        array_filters: List[Dict[str, Any]] = ...,
        hint: Optional[_Hint] = ...,
    ) -> None: ...

class IndexModel:
    def __init__(
        self,
        keys: _Hint,
        **kwargs: Any,
    ) -> None: ...
