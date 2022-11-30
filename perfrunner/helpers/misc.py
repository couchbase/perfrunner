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


def inject_config_tags(config_path,
                       operator_tag,
                       admission_controller_tag):
    #  operator
    with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/operator:build'
        replace = operator_tag
        for line in file:
            print(line.replace(search, replace), end='')

    #  admission controller
    with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/admission-controller:build'
        replace = admission_controller_tag
        for line in file:
            print(line.replace(search, replace), end='')


def inject_cluster_tags(cluster_path,
                        couchbase_tag,
                        operator_backup_tag,
                        exporter_tag):
    #  couchbase
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/server:build'
        replace = couchbase_tag
        for line in file:
            print(line.replace(search, replace), end='')

    #  operator backup
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/operator-backup:build'
        replace = operator_backup_tag
        for line in file:
            print(line.replace(search, replace), end='')

    #  exporter
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/exporter:build'
        replace = exporter_tag
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


def is_null(element) -> bool:
    if (isinstance(element, int) or isinstance(element, float)) and element == 0:
        return False
    elif isinstance(element, bool):
        return False
    else:
        return False if element else True


def remove_nulls(d: dict) -> dict:
    if not isinstance(d, dict):
        return d
    return {k: new_v for k, v in d.items() if not is_null(new_v := remove_nulls(v))}


def use_ssh_capella(spec) -> bool:
    # Disable SSH for Capella if we aren't on AWS or if we are in capella-dev tenant,
    # as this tenant has a higher level of security and we don't have permissions to do many
    # things there
    return (
        spec.capella_infrastructure and
        spec.capella_backend == 'aws' and
        spec.infrastructure_settings['cbc_tenant'] != '1a3c4544-772e-449e-9996-1203e7020b96'
    )
