class NotRegistered(Exception):
    pass

class InvalidDocumentError(Exception):
    pass

class DoesNotExist(Exception):
    pass

class FieldDoesNotExist(Exception):
    pass

class OperationError(Exception):
    pass

class NotUniqueError(OperationError):
    pass

class MultipleObjectsReturned(Exception):
    pass

class InvalidQueryError(Exception):
    pass

class LookUpError(Exception):
    pass

class ValidationError(AssertionError):
    message: str

__all__ = [
    "NotRegistered",
    "DoesNotExist",
    "FieldDoesNotExist",
    "NotUniqueError",
    "OperationError",
    "InvalidQueryError",
    "LookUpError",
    "ValidationError",
    "MultipleObjectsReturned",
]
