from typing import Any, Optional

PYTHON_LEGACY: int

class CodecOptions:
    def __init__(
        self,
        document_class: Any,
        tz_aware: bool = ...,
        uuid_representation: int = ...,
        unicode_decode_error_handler: str = ...,
        tzinfo: Optional[Any] = ...,
    ) -> None: ...
