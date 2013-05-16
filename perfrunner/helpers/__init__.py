from perfrunner.config import ClusterSpec, TestConfig


class Helper(object):

    def __init__(self, cluster_spec_fname=None, test_config_fname=None):
        if cluster_spec_fname:
            config = ClusterSpec()
            config.parse(cluster_spec_fname)

            self.clusters = config.get_clusters()
            self.hosts = config.get_hosts()
            self.data_path, self.index_path = config.get_paths()
            self.ssh_username, self.ssh_password = config.get_ssh_credentials()
            self.rest_username, self.rest_password = config.get_rest_credentials()

        if test_config_fname:
            config = TestConfig()
            config.parse(test_config_fname)

            self.mem_quota = config.get_mem_quota()
            self.initial_nodes = config.get_initial_nodes()
            self.num_buckets = config.get_num_buckets()
            self.compaction_settings = config.get_compaction_settings()
