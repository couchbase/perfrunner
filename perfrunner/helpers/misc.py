import json
import time
from hashlib import md5
from uuid import uuid4


def uhex():
    return uuid4().hex


def pretty_dict(d):
    return json.dumps(d, indent=4, sort_keys=True,
                      default=lambda o: o.__dict__)


def target_hash(*args):
    int_hash = hash(args)
    str_hash = md5(hex(int_hash).encode('utf-8')).hexdigest()
    return str_hash[:6]


def retry(catch=(), iterations=5, wait=10):
    """
    This is a general purpose decorator for retrying a function while
    discarding a tuple of exceptions that the function might throw.

    'catch' is a tuple of exceptions. Passing in a list is also fine.

    'iterations' means number of total attempted calls. 'iterations' is only
    meaningful when >= 2.

    'wait' is wait time between calls.

    Usage:

    import perfrunner.helpers.misc

    @perfrunner.helpers.misc.retry(catch=[RuntimeError, KeyError])
    def hi():
        raise KeyError("Key Errrrr from Hi")

    # or if you want to tune your own iterations and wait

    @perfrunner.helpers.misc.retry(
        catch=[KeyError, TypeError],
        iterations=3, wait=1)
    def hi(who):
        print "hi called"
        return "hi " +  who

    print hi("john")
    # this throws TypeError when 'str' and 'None are concatenated
    print hi(None)
    """
    # in case the user specifies a list of Exceptions instead of a tuple
    catch = tuple(catch)

    def retry_decorator(func):
        def retry_wrapper(*arg, **kwargs):
            for i in range(iterations):
                try:
                    result = func(*arg, **kwargs)
                except catch:
                    if i == (iterations - 1):
                        raise
                    else:
                        pass
                else:
                    return result
                time.sleep(wait)
        return retry_wrapper
    return retry_decorator


def read_json(filename: str):
    with open(filename) as fh:
        return json.load(fh)
