import os
import sys
import time
from functools import wraps

from pymongo.errors import ConnectionFailure


def retry_upon_connection_failure(func):
    """
    Decorator to retry operations on connection failures.
    It determines the retry count from the environment variable `MONGOENGINE_RETRY_COUNT`
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        tries = int(os.environ.get('MONGOENGINE_RETRY_COUNT', 4))
        for retry_count in range(1, tries + 1):
            try:
                return func(*args, **kwargs)
            except:
                if isinstance(sys.exc_info()[0], ConnectionFailure) and retry_count >= tries:
                    raise
                time.sleep(1)
    return wrapper
