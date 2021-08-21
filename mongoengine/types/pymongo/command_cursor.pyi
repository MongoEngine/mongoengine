from typing import Any, Dict, Iterator

class CommandCursor:
    def __iter__(self) -> Iterator[Dict[str, Any]]: ...
    def __next__(self) -> Dict[str, Any]: ...

__all__ = ["CommandCursor"]
