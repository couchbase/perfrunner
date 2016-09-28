from fabric.api import run
from logger import logger


class Remote(object):

    def __init__(self, cluster_spec, test_config, os):
        self.os = os
        self.hosts = tuple(cluster_spec.yield_hostnames())
        self.kv_hosts = tuple(cluster_spec.yield_kv_servers())
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    @staticmethod
    def wget(url, outdir='/tmp', outfile=None):
        logger.info('Fetching {}'.format(url))
        if outfile is not None:
            run('wget -nc "{}" -P {} -O {}'.format(url, outdir, outfile))
        else:
            run('wget -N "{}" -P {}'.format(url, outdir))
