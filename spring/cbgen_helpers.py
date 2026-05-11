import random
from collections import defaultdict
from threading import Timer
from time import sleep, time
from typing import Callable, Optional, Union
from urllib import parse

import pkg_resources
from decorator import decorator
from requests import HTTPError

from logger import logger

sdk_major_version = int(pkg_resources.get_distribution("couchbase").version[0])

if sdk_major_version == 2:
    from couchbase.exceptions import CouchbaseError, TemporaryFailError
elif sdk_major_version >= 3:
    from couchbase.exceptions import CouchbaseException as CouchbaseError
    from couchbase.exceptions import TemporaryFailException as TemporaryFailError


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


class QueryFailureTracker:
    """Passive per-worker counter for N1QL query failures during an access phase.

    Accumulates per-reason counts inside a single worker process. Call
    log_summary() at worker shutdown to emit a consolidated failure report.
    Does not affect KPI calculation or reservoir sampling.
    """

    MAX_MSG_LEN = 120
    MAX_STMT_LEN = 80

    def __init__(self):
        self.total = 0
        self.by_reason: dict = defaultdict(int)

    def track(self, exc: ClientError, statement: str = '') -> None:
        self.total += 1
        key = (type(exc).__name__, str(exc)[:self.MAX_MSG_LEN], statement[:self.MAX_STMT_LEN])
        self.by_reason[key] += 1

    def log_summary(self, worker_id: int) -> None:
        if not self.total:
            return
        lines = ['N1QL query failure summary for query-worker-{}: {} failed call(s)'.format(
            worker_id, self.total)]
        for (exc_type, msg, stmt_hint), count in sorted(
                self.by_reason.items(), key=lambda kv: -kv[1]):
            hint = ' [stmt: {}]'.format(stmt_hint) if stmt_hint else ''
            lines.append('  {} x {} — {}{}'.format(count, exc_type, msg, hint))
        logger.warn('\n'.join(lines))


query_failure_tracker = QueryFailureTracker()


def _extract_n1ql_statement(decorated_args: tuple) -> str:
    """Return a short statement hint from n1ql_query decorated call args.

    Handles SDK 2 (N1QLQuery object with .statement attr) and SDK 3/4
    (plain string). decorated_args[0] is the CBGen instance;
    decorated_args[1] is the query argument.
    """
    if len(decorated_args) < 2:
        return ''
    query_arg = decorated_args[1]
    if isinstance(query_arg, str):
        return query_arg[:80]
    stmt = getattr(query_arg, 'statement', None)
    if stmt:
        return str(stmt)[:80]
    return ''


@decorator
def quiet(method: Callable, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except CouchbaseError as e:
        error_tracker.track(method.__name__, e)
        if method.__name__ == 'n1ql_query':
            query_failure_tracker.track(e, _extract_n1ql_statement(args))
    except HTTPError as e:
        error_tracker.track(method.__name__, e)
        if method.__name__ == 'n1ql_query':
            query_failure_tracker.track(e, _extract_n1ql_statement(args))


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


def get_connection(**kwargs) -> tuple[str, Optional[str]]:
    """Create a desired combination of connection string and certificate from the kwargs input."""
    scheme = "couchbase"
    cert_path = None
    connstr_params: dict = kwargs.get("connstr_params", {})
    ssl_mode = kwargs.get("ssl_mode", "none")
    if ssl_mode in ["data", "n2n", "capella"]:
        scheme = "couchbases"
        if ssl_mode in ["capella"]:
            connstr_params.update({"ssl": "no_verify"})
        else:
            cert_path = "root.pem"
        connstr_params.update({"sasl_mech_force": "PLAIN"})

    encoded_params = parse.urlencode(connstr_params)
    host = kwargs.get("host", "localhost")
    return f"{scheme}://{host}?{encoded_params}", cert_path
