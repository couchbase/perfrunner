import unittest

from perfrunner.__main__ import get_options
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.tests import TargetIterator


class FunctionalTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        options, _args = get_options()
        override = \
            _args and (arg.split('.') for arg in ' '.join(_args).split(','))

        self.cluster_spec = ClusterSpec()
        self.cluster_spec.parse(options.cluster_spec_fname)
        self.test_config = TestConfig()
        self.test_config.parse(options.test_config_fname, override)

        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config)
        self.memcached = MemcachedHelper(self.test_config)
        self.remote = RemoteHelper(self.cluster_spec, self.test_config)
        self.rest = RestHelper(self.cluster_spec)

        super(FunctionalTest, self).__init__(*args, **kwargs)


class MemcachedTests(FunctionalTest):

    def test_num_threads(self):
        expected_threads = self.test_config.cluster.num_cpus
        if expected_threads is None:
            cores = self.remote.detect_number_cores()
            expected_threads = int(0.75 * cores)
        for target in self.target_iterator:
            host = target.node.split(':')[0]
            port = self.rest.get_memcached_port(target.node)
            stats = self.memcached.get_stats(host, port, target.bucket,
                                             stats='')
            num_threads = int(stats['threads'])
            self.assertEqual(num_threads, expected_threads)

if __name__ == '__main__':
    unittest.main(argv=['functional.py'])
