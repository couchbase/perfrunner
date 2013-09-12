import json
from uuid import uuid4

from logger import logger


def uhex():
    return uuid4().hex


def pretty_dict(d):
    return json.dumps(d, indent=4, sort_keys=True)


def log_phase(phase, settings):
    logger.info('Running {0}: {1}'.format(phase, pretty_dict(settings)))
