import random
from collections import defaultdict
from threading import Timer
from time import sleep, time
from typing import Callable, Union

import pkg_resources
from decorator import decorator
from requests import HTTPError

from logger import logger

cb_version = pkg_resources.get_distribution("couchbase").version

if cb_version[0] == '2':
    from couchbase.exceptions import CouchbaseError, TemporaryFailError
elif cb_version[0] == '3':
    from couchbase.exceptions import CouchbaseException as CouchbaseError
    from couchbase.exceptions import \
        TemporaryFailException as TemporaryFailError


ClientError = Union[CouchbaseError, HTTPError]


class ErrorTracker:

    MSG = 'Function: {}, error: {}'

    QUIET_PERIOD = 10  # 10 seconds

    def __init__(self):
        self.errors = defaultdict(int)

    def track(self, method: str, exc: ClientError):
        if type(exc) not in self.errors:
            self.warn(method, exc)  # Always warn upon the first occurrence
            self.check_later(method, exc)
        self.incr(exc)

    def incr(self, exc: ClientError):
        self.errors[type(exc)] += 1

    def reset(self, exc: ClientError):
        self.errors[type(exc)] = 0

    def check_later(self, method: str, exc: ClientError):
        timer = Timer(self.QUIET_PERIOD, self.maybe_warn, args=[method, exc])
        timer.daemon = True
        timer.start()

    def warn(self, method: str, exc: ClientError, count: int = 0):
        message = self.MSG.format(method, exc)
        if isinstance(exc, HTTPError):
            message += ', response text: {}'.format(exc.response.text)
        if count:
            message += ', repeated {} times'.format(count)
        logger.warn(message)

    def maybe_warn(self, method: str, exc: ClientError):
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
    except HTTPError as e:
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
    except HTTPError as e:
        error_tracker.track(method.__name__, e)
