from typing import Any, Callable

SignalCallback = Callable[[Any, Any], None]

class post_save:
    @staticmethod
    def connect(callback: SignalCallback, sender: object) -> None: ...

class post_delete:
    @staticmethod
    def connect(callback: SignalCallback, sender: object) -> None: ...

class pre_save_post_validation:
    @staticmethod
    def connect(callback: SignalCallback, sender: object) -> None: ...
