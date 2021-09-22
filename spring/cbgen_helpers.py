import random
from collections import defaultdict
from threading import Timer
from time import sleep, time
from typing import Callable

import pkg_resources
from decorator import decorator

from logger import logger

cb_version = pkg_resources.get_distribution("couchbase").version

if cb_version[0] == '2':
    from couchbase.exceptions import CouchbaseError, TemporaryFailError
elif cb_version[0] == '3':
    from couchbase.exceptions import CouchbaseException as CouchbaseError
    from couchbase.exceptions import \
        TemporaryFailException as TemporaryFailError


class ErrorTracker:

    MSG = 'Function: {}, error: {}'

    MSG_REPEATED = 'Function: {}, error: {}, repeated {} times'

    QUIET_PERIOD = 10  # 10 seconds

    def __init__(self):
        self.errors = defaultdict(int)

    def track(self, method: str, exc: CouchbaseError):
        if type(exc) not in self.errors:
            self.warn(method, exc)  # Always warn upon the first occurrence
            self.check_later(method, exc)
        self.incr(exc)

    def incr(self, exc: CouchbaseError):
        self.errors[type(exc)] += 1

    def reset(self, exc: CouchbaseError):
        self.errors[type(exc)] = 0

    def check_later(self, method: str, exc: CouchbaseError):
        timer = Timer(self.QUIET_PERIOD, self.maybe_warn, args=[method, exc])
        timer.daemon = True
        timer.start()

    def warn(self, method: str, exc: CouchbaseError, count: int = 0):
        if count:
            logger.warn(self.MSG_REPEATED.format(method, exc, count))
        else:
            logger.warn(self.MSG.format(method, exc))

    def maybe_warn(self, method: str, exc: CouchbaseError):
        count = self.errors[type(exc)]
        if count > 1:
            self.reset(exc)
            self.warn(method, exc, count)
            self.check_later(method, exc)
        else:  # Not repeated, hence stop tracking it
            self.errors.pop(type(exc))


error_tracker = ErrorTracker()


@decorator
def quiet(method: Callable, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except CouchbaseError as e:
        error_tracker.track(method.__name__, e)


@decorator
def backoff(method: Callable, *args, **kwargs):
    retry_delay = 0.1  # Start with 100 ms
    while True:
        try:
            return method(*args, **kwargs)
        except TemporaryFailError:
            sleep(retry_delay)
            # Increase exponentially with jitter
            retry_delay *= 1 + 0.1 * random.random()


@decorator
def timeit(method: Callable, *args, **kwargs) -> float:
    t0 = time()
    method(*args, **kwargs)
    return time() - t0


@decorator
def time_all(method: Callable, *args, **kwargs):
    # Needs to be tidied, includes backoff+quiet functions, to be reduced to a smaller decorator.
    try:
        retry_delay = 0.1
        has_retried = False
        start_time = time()
        while True:
            try:
                t0 = time()
                method(*args, **kwargs)
                t1 = time()
                if has_retried:
                    ret_pair = (t1 - t0, t1 - start_time)
                    return ret_pair
                else:
                    ret_pair = (t1 - t0, t1 - t0)
                    return ret_pair
            except TemporaryFailError:
                has_retried = True
                sleep(retry_delay)
                retry_delay *= 1 + 0.1 * random.random()
    except CouchbaseError as e:
        error_tracker.track(method.__name__, e)
