from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
import pymongo

UPDATE_OPERATORS = Set[str]

class BaseField:
    def __new__(
        cls,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[Any, None, Callable[[], Any]] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: bool = ...,
        choices: Optional[Iterable[Any]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
    ) -> BaseField: ...
    def __set__(self, instance: Any, value: Any) -> None: ...
    def __get__(self, instance: Any, owner: Any) -> Any: ...
    def validate(self, value: Any, clean: bool = ...) -> None: ...

class ObjectIdField(BaseField): ...
class ComplexBaseField(BaseField): ...
class GeoJsonBaseField(BaseField):
    _geo_index = pymongo.GEOSPHERE
    _type = "GeoBase"

