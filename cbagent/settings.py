import json
import os

from logger import logger


class Settings(object):

    DEFAULT = {
        "cbmonitor_host_port": "127.0.0.1:8000",
        "seriesly_host": "127.0.0.1",

        "interval": 10,

        "cluster": "default",
        "master_node": "127.0.0.1",
        "dest_master_node": "127.0.0.1",
        "rest_username": "Administrator",
        "rest_password": "password",
        "bucket_password": "password",
        "ssh_username": "root",
        "ssh_password": "couchbase",
        "partitions": {},

        "buckets": None,
        "hostnames": None
    }

    def __init__(self, options={}):
        for option, value in dict(self.DEFAULT, **options).items():
            setattr(self, option, value)

    def read_cfg(self, config):
        if not os.path.isfile(config):
            logger.interrupt("File doesn\'t exist: {}".format(config))

        logger.info("Reading configuration file: {}".format(config))
        with open(config) as fh:
            try:
                for option, value in json.load(fh).items():
                    setattr(self, option, value)
            except ValueError as e:
                logger.interrupt("Error reading config: {}".format(e))
            else:
                logger.info("Configuration file successfully parsed")
