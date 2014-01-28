from couchbase import Couchbase

from logger import logger

from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import SF_STORAGE


class ExperimentHelper(object):

    INPUTS = {
        'Source nodes': 'self.tc.get_initial_nodes()[0]',
        'Destination nodes': 'self.tc.get_initial_nodes()[1]',
        'Mutations/sec': '0.8 * self.tc.get_access_settings().throughput',
        'Number of buckets': 'self.tc.get_num_buckets()',
        'Number of vbuckets': 'self.tc.get_num_vbuckets()',
        'Number of items': 'self.tc.get_load_settings().items',
        'Number of replicas': 'self.tc.get_replica_number()',

        'Drive type': 'self.cs.get_parameters()["Disk"].split()[-1]',
    }

    def __init__(self, experiment, cluster_spec, test_config):
        self.name = experiment.name
        self.experiment = experiment.template
        self.tc = test_config
        self.cs = cluster_spec

        self.experiment['inputs'] = {
            param: eval(self.INPUTS[param])
            for param in self.experiment['defaults']
        }

    def update_defaults(self):
        cb = Couchbase.connect(bucket='exp_defaults', **SF_STORAGE)
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
        cb = Couchbase.connect(bucket='experiments', **SF_STORAGE)
        cb.set(key, self.experiment)
