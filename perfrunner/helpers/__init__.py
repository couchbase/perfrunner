from perfrunner.config import ClusterSpec, TestConfig


class Helper(object):

    def __init__(self, cluster_spec=None, test_config=None):
        if cluster_spec:
            config = ClusterSpec()
            config.parse(cluster_spec)
            self.__dict__.update(config.__dict__)
        if test_config:
            config = TestConfig()
            config.parse(test_config)
            self.__dict__.update(config.__dict__)
            self.test_config = config
