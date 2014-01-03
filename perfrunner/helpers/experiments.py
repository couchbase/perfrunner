from couchbase import Couchbase

from perfrunner.helpers.misc import uhex
from perfrunner.settings import SF_STORAGE


class ExperimentHelper(object):

    INPUTS = {
        'Source nodes': 'self.test_config.get_initial_nodes()[0]',
        'Destination nodes': 'self.test_config.get_initial_nodes()[1]',
    }

    def __init__(self, experiment, test_config):
        self.name = experiment.name
        self.experiment = experiment.template
        self.test_config = test_config

        self.experiment['input'] = {
            self.experiment['input']['param']: eval(self.INPUTS[param])
            for param in experiment['defaults']
        }

    def update_defaults(self):
        cb = Couchbase.connect(bucket='exp_defaults', **SF_STORAGE)
        cb.set(self.name, self.experiment['defaults'])

    def post_results(self, value):
        self.update_defaults()

        self.experiment['value'] = value
        self.experiment['defaults'] = self.experiment.name

        cb = Couchbase.connect(bucket='experiments', **SF_STORAGE)
        cb.set(uhex(), self.experiment)
