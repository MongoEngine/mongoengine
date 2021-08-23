class CommandListener:
    pass

class CommandStartedEvent:
    pass

class CommandSucceededEvent:
    duration_micros: int
    command_name: str

class CommandFailedEvent:
    duration_micros: int
    command_name: str

def register(listener: CommandListener) -> None: ...
