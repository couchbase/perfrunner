from couchbase import Couchbase

from perfrunner.helpers.misc import uhex
from perfrunner.settings import SF_STORAGE


class ExperimentHelper(object):

    INPUTS = {
        'Source nodes': 'self.tc.get_initial_nodes()[0]',
        'Destination nodes': 'self.tc.get_initial_nodes()[1]',
        'Mutations/sec': '0.7 * self.tc.get_access_settings().throughput',

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

        self.experiment['value'] = value
        self.experiment['defaults'] = self.name

        cb = Couchbase.connect(bucket='experiments', **SF_STORAGE)
        cb.set(uhex(), self.experiment)
