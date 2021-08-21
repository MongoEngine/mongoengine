from typing import Any

class Collation:
    def __init__(self, locale: str, strength: Any, caseLevel: bool) -> None: ...

class CollationStrength:
    PRIMARY: object
