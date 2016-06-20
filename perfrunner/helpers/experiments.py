from couchbase import Couchbase
from logger import logger

from perfrunner.helpers.misc import pretty_dict, uhex


class ExperimentHelper(object):

    INPUTS = {
        'Source nodes': 'self.tc.cluster.initial_nodes[0]',
        'Destination nodes': 'self.tc.cluster.initial_nodes[1]',
        'Mutations/sec': '0.8 * self.tc.access_settings.throughput',
        'Number of buckets': 'self.tc.cluster.num_buckets',
        'Number of vbuckets': 'self.tc.cluster.num_vbuckets',
        'Number of items (10e6)': 'self.tc.load_settings.items / 10 ** 6',
        'Number of replicas': 'self.tc.bucket.replica_number',
        'Moves per node': 'self.tc.internal_settings["rebalanceMovesPerNode"]',
        'Moxi': 'self.tc.cluster.disable_moxi is None and "Moxi on" or "Moxi off"',
        'Compaction': 'self.tc.compaction.db_percentage == "100" and "Off" or "On"',
        'Value size': 'self.tc.load_settings.size',
        'memcached threads': 'self.tc.cluster.num_cpus',

        'Drive type': 'self.cs.parameters["Disk"].split()[-1]',
    }

    def __init__(self, experiment, cluster_spec, test_config):
        self.name = experiment.name
        self.experiment = experiment.template
        self.tc = test_config
        self.cs = cluster_spec
        self.cbmonitor = test_config.stats_settings.cbmonitor

        self.experiment['inputs'] = {
            param: eval(self.INPUTS[param])
            for param in self.experiment['defaults']
        }

    def update_defaults(self):
        cb = Couchbase.connect(bucket='exp_defaults', **self.cbmonitor)
        cb.set(self.name, {
            'id': self.name,
            'name': self.experiment['name'],
            'inputs': self.experiment['defaults'],
        })

    def post_results(self, value):
        self.update_defaults()

        key = uhex()
        self.experiment['value'] = value
        self.experiment['defaults'] = self.name

        logger.info('Adding new experiment {}: {}'.format(
            key, pretty_dict(self.experiment)
        ))
        cb = Couchbase.connect(bucket='experiments', **self.cbmonitor)
        cb.set(key, self.experiment)
