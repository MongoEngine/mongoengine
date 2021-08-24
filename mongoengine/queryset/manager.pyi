from typing import TypeVar, Callable, Type
from mongoengine import Document
from mongoengine.queryset import QuerySet

_DT = TypeVar('_DT', bound=Document)
class QuerySetManager(object): 
    def __get__(self, instance: object, cls: Type[_DT]) -> QuerySet[_DT]: ...

def queryset_manager(func: Callable[[Type[_DT], QuerySet[_DT]], QuerySet[_DT]]) -> QuerySetManager: ...
