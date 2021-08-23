import datetime
from typing import Any, Union

UPPERBOUND: int


class Timestamp:
    _type_marker: int

    def __init__(self, time: Union[int, datetime.datetime], inc: int) -> None:
        ...

    @property
    def time(self) -> int:
        ...

    @property
    def inc(self) -> int:
        ...

    def __eq__(self, other: Any) -> bool:
        ...

    def __hash__(self) -> int:
        ...

    def __ne__(self, other: Any) -> bool:
        ...

    def __lt__(self, other: Any) -> bool:
        ...

    def __le__(self, other: Any) -> bool:
        ...

    def __gt__(self, other: Any) -> bool:
        ...

    def __ge__(self, other: Any) -> bool:
        ...

    def __repr__(self) -> str:
        ...

    def as_datetime(self) -> datetime.datetime:
        ...
