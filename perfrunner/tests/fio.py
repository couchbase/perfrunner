from collections import defaultdict

from logger import logger

from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.tests import PerfTest


class FIOTest(PerfTest):

    def __init__(self, cluster_spec, test_config, verbose):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)

    @staticmethod
    def _parse(results):
        """Terse output parsing is based on the following guide:

            https://github.com/axboe/fio/blob/master/HOWTO
        """
        stats = defaultdict(int)
        for host, output in results.items():
            for job in output.split():
                stats[host] += int(job.split(';')[7])  # reads
                stats[host] += int(job.split(';')[48])  # writes
        return stats

    def run(self):
        stats = self.remote.fio(self.test_config.fio['config'])
        logger.info('IOPS: {}'.format(pretty_dict(self._parse(stats))))
