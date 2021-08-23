from bson.codec_options import CodecOptions
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.write_concern import WriteConcern
from typing import Any
from typing_extensions import Literal

MIN_SUPPORTED_WIRE_VERSION: Literal[2] = 2
MAX_SUPPORTED_WIRE_VERSION: Literal[9] = 9

class BaseObject:
    @property
    def codec_options(self) -> CodecOptions: ...
    @property
    def write_concern(self) -> WriteConcern: ...
    @property
    def read_preference(self) -> ReadPreference: ...
    @property
    def read_concern(self) -> ReadConcern: ...

def validate_read_preference(dummy: Any, value: _ServerMode) -> _ServerMode: ...
