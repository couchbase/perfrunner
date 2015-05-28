import json
from hashlib import md5
from uuid import uuid4

from logger import logger


def uhex():
    return uuid4().hex


def pretty_dict(d):
    return json.dumps(d, indent=4, sort_keys=True,
                      default=lambda o: o.__dict__)


def log_phase(phase, settings):
    logger.info('Running {}: {}'.format(phase, pretty_dict(settings)))


def target_hash(*args):
    int_hash = hash(args)
    str_hash = md5(hex(int_hash)).hexdigest()
    return str_hash[:6]


def server_group(servers, group_number, i):
    group_id = 1 + i / ((len(servers) + 1) / group_number)
    return 'Group {}'.format(group_id)
