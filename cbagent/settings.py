from perfrunner.settings import CBMONITOR_HOST
from perfrunner.tests import PerfTest


class CbAgentSettings:

    def __init__(self, test: PerfTest):
        if hasattr(test, 'ALL_BUCKETS') and getattr(test, 'ALL_BUCKETS'):
            buckets = None
        else:
            buckets = test.test_config.buckets +\
                test.test_config.eventing_buckets +\
                test.test_config.eventing_metadata_bucket

        if hasattr(test, 'ALL_HOSTNAMES'):
            hostnames = test.cluster_spec.servers
            if test.cluster_spec.infrastructure_syncgateways:
                hostnames += test.cluster_spec.sgw_servers
        else:
            hostnames = None

        self.cbmonitor_host = CBMONITOR_HOST
        self.interval = test.test_config.stats_settings.interval
        self.lat_interval = test.test_config.stats_settings.lat_interval
        self.buckets = buckets
        self.collections = None
        self.indexes = {}
        self.hostnames = hostnames
        self.secondary_statsfile = test.test_config.stats_settings.secondary_statsfile
        self.client_processes = test.test_config.stats_settings.client_processes
        self.server_processes = test.test_config.stats_settings.server_processes
        self.traced_processes = test.test_config.stats_settings.traced_processes
        self.workers = test.cluster_spec.workers
        self.cloud = {"enabled": False}
        self.serverless_db_names = {}

        self.remote = False
        if test.test_config.test_case.use_workers:
            self.remote = test.worker_manager.is_remote

        self.ssh_username, self.ssh_password = test.cluster_spec.ssh_credentials
        self.rest_username, self.rest_password = test.cluster_spec.rest_credentials
        self.bucket_username, self.bucket_password = test.cluster_spec.rest_credentials

        if test.dynamic_infra:
            self.cloud = {"enabled": True, "dynamic": True, "cloud_rest": test.rest}
        elif test.cluster_spec.cloud_infrastructure:
            self.cloud.update({'enabled': True, 'dynamic': False, 'cloud_rest': test.rest})
            self.hostnames = test.cluster_spec.servers
            if test.cluster_spec.serverless_infrastructure:
                self.cloud.update({'serverless': True})
                self.serverless_db_names = {
                    k: v['name'] for k, v in test.test_config.serverless_db.db_map.items()
                }

        if test.test_config.collection.collection_map:
            self.collections = test.test_config.collection.collection_map

        if test.cluster_spec.servers_by_role('index'):
            if test.test_config.gsi_settings.indexes:
                self.indexes = test.test_config.gsi_settings.indexes
            else:
                self.indexes = test.test_config.index_settings.indexes
            self.index_node = test.cluster_spec.servers_by_role('index')[0]
        self.is_n2n = False
        if test.test_config.cluster.enable_n2n_encryption is not None:
            self.is_n2n = True

        if self.remote:
            self.remote_worker_home = test.worker_manager.WORKER_HOME
        self.capella_infrastructure = test.cluster_spec.capella_infrastructure
