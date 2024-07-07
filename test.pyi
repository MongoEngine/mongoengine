from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import reveal_type

_ST = TypeVar("_ST")
_GT = TypeVar("_GT")
_T = TypeVar("_T")

class EnumField(Generic[_ST, _GT]):
    def __init__(
        self,
        enum: Type[_T],
        required: bool = True,
        default: Any = None,
        help_text: str | None = None,
    ) -> None: ...

    # EnumField(Foo)
    @overload
    def __new__(
        cls,
        enum: Type[_T],
        *,
        db_field: str = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Any,
    ) -> EnumField[Optional[_T], Optional[_T]]: ...
    # EnumField(Foo, default=Foo.Bar)
    @overload
    def __new__(
        cls,
        enum: Type[_T],
        *,
        required: Literal[False] = ...,
        default: Union[_T, Callable[[], _T]],
        **kwargs: Any,
    ) -> EnumField[Optional[_T], _T]: ...
    # EnumField(Foo, required=True)
    @overload
    def __new__(
        cls,
        enum: Type[_T],
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Any,
    ) -> EnumField[_T, _T]: ...
    # EnumField(Foo, required=True, default=Foo.Bar)
    @overload
    def __new__(
        cls,
        enum: Type[_T],
        *,
        required: Literal[True],
        default: Union[_T, Callable[[], _T]],
        **kwargs: Any,
    ) -> EnumField[Optional[_T], _T]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...
    def __get__(self, instance: Any, owner: Any) -> _GT: ...

class Foo: ...

class Bar:
    status = EnumField(Foo, required=True)

b = Bar()
reveal_type(b.status)
