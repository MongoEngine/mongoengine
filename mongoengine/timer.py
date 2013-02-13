import time
from contextlib import contextmanager

callback = None

SLOW_THRESHOLD = 100

@contextmanager
def log_slow_event(event_name, collection, params, threshold=None):
    start_time = time.time()

    yield

    run_time = 1000.0 * (time.time() - start_time)

    if threshold is None:
        threshold = SLOW_THRESHOLD

    if run_time > threshold and callback:
        callback(event_name, collection, params, run_time)

def set_slow_event_callback(new_callback):
    global callback
    callback = new_callback
