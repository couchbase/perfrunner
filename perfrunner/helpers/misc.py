import fileinput
import json
import shutil
import time
from hashlib import md5
from typing import Any, Union
from uuid import uuid4


def uhex() -> str:
    return uuid4().hex


def pretty_dict(d: Any) -> str:
    return json.dumps(d, indent=4, sort_keys=True,
                      default=lambda o: o.__dict__)


def target_hash(*args: str) -> str:
    int_hash = hash(args)
    str_hash = md5(hex(int_hash).encode('utf-8')).hexdigest()
    return str_hash[:6]


def retry(catch: tuple = (), iterations: int = 5, wait: int = 10):
    """Retry a function while discarding the specified exceptions.

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


def read_json(filename: str) -> dict:
    with open(filename) as fh:
        return json.load(fh)


def maybe_atoi(a: str, t=int) -> Union[int, float, str, bool]:
    if a.lower() == 'false':
        return False
    elif a.lower() == 'true':
        return True
    else:
        try:
            return t(a)
        except ValueError:
            return a


def human_format(number: float) -> str:
    magnitude = 0
    while abs(number) >= 1e3:
        magnitude += 1
        number /= 1e3
    return '{:.0f}{}'.format(number, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])


def copy_template(source, dest):
    shutil.copyfile(source, dest)


def inject_operator_build(config_path, release, build):
    repo = 'couchbase'
    operator_name = 'couchbase-operator-internal'
    admission_controller_name = 'couchbase-admission-internal'
    major = int(release[0])
    minor = int(release[2])
    if major >= 2 and minor >= 1:
        repo = 'registry.gitlab.com/cb-vanilla'
        operator_name = 'operator'
        admission_controller_name = 'admission-controller'
    with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/operator:build'
        replace = '{}/{}:{}-{}'.format(repo, operator_name, release, build)
        for line in file:
            print(line.replace(search, replace), end='')
    with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/admission-controller:build'
        replace = '{}/{}:{}-{}'.format(repo, admission_controller_name, release, build)
        for line in file:
            print(line.replace(search, replace), end='')


def inject_couchbase_version(cluster_path, couchbase_version):
    repo = 'couchbase'
    backup_repo = 'registry.gitlab.com/cb-vanilla/operator-backup'
    release = couchbase_version.split("-")[0]
    major = int(release[0])
    minor = int(release[2])
    patch = int(release[4])
    is_build = "-" in couchbase_version
    if is_build:
        build = int(couchbase_version.split("-")[1])
        check_66 = major == 6 and minor == 6
        check_660 = check_66 and patch == 0 and build >= 7924
        check_661 = check_66 and patch == 1 and build >= 9133
        check_700 = major >= 7 and minor >= 0 and patch >= 0
        if check_660 or check_661 or check_700:
            repo = 'registry.gitlab.com/cb-vanilla/server'
        else:
            repo = 'couchbase/server-internal'
        if major <= 6 and minor <= 5:
            backup_build = '6.5.1-116'
        else:
            backup_build = '6.6.0-100'
    else:
        repo = 'couchbase/server'
        backup_build = '6.6.0-100'

    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/server:build'
        replace = '{}:{}'.format(repo, couchbase_version)
        for line in file:
            print(line.replace(search, replace), end='')

    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/operator-backup:build'
        replace = '{}:{}'.format(backup_repo, backup_build)
        for line in file:
            print(line.replace(search, replace), end='')


def inject_server_count(cluster_path, server_count):
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'size: node_count'
        replace = 'size: {}'.format(server_count)
        for line in file:
            print(line.replace(search, replace), end='')


def inject_num_workers(num_workers, worker_template_path, worker_path):
    shutil.copyfile(worker_template_path, worker_path)
    with fileinput.FileInput(worker_path, inplace=True, backup='.bak') as file:
        search = 'NUM_WORKERS'
        replace = '{}'.format(str(num_workers))
        for line in file:
            print(line.replace(search, replace), end='')
