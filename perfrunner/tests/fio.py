from collections import defaultdict

import requests
from logger import logger

from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.tests import PerfTest


class FIOTest(PerfTest):

    TRACKER = 'fio.sc.couchbase.com'

    TEMPLATE = {
        'group': '{}, random mixed reads and writes, IOPS',
        'metric': None,
        'value': None,
    }

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

    def _post(self, data):
        data = pretty_dict(data)
        logger.info('Posting: {}'.format(data))
        requests.post('http://{}/api/v1/benchmarks'.format(self.TRACKER),
                      data=data)

    def _report_kpi(self, stats):
        for host, iops in stats.items():
            data = self.TEMPLATE.copy()
            data['group'] = data['group'].format(self.cluster_spec.name.title())
            data['metric'] = host
            data['value'] = iops

            self._post(data)

    def run(self):
        stats = self.remote.fio(self.test_config.fio['config'])
        self._report_kpi(self._parse(stats))
