class Helper(object):

    def __init__(self, cluster_spec=None, test_config=None):
        if cluster_spec:
            self.clusters = cluster_spec.get_clusters()
            self.hosts = cluster_spec.get_hosts()
            self.data_path, self.index_path = cluster_spec.get_paths()
            self.ssh_username, self.ssh_password = \
                cluster_spec.get_ssh_credentials()
            self.rest_username, self.rest_password = \
                cluster_spec.get_rest_credentials()

        if test_config:
            self.mem_quota = test_config.get_mem_quota()
            self.num_threads = test_config.get_num_mrw_threads()
            self.initial_nodes = test_config.get_initial_nodes()
            self.num_buckets = test_config.get_num_buckets()
            self.compaction_settings = test_config.get_compaction_settings()
