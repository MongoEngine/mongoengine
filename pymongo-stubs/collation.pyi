from typing import Any, Optional

class Collation:
    def __init__(self, locale: str, strength: Any, caseLevel: Optional[bool] = None) -> None: ...

class CollationStrength:
    PRIMARY: object
