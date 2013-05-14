from perfrunner.utils.config import ClusterSpec


class Helper(object):

    def __init__(self, spec_file):
        config = ClusterSpec()
        config.parse(spec_file)
        self.__dict__ = config.__dict__
