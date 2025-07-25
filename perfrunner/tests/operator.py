import datetime
import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.cluster import KubernetesClusterManager
from perfrunner.helpers.config_files import TimeTrackingFile
from perfrunner.remote.kubernetes import RemoteKubernetes
from perfrunner.settings import ClusterSpec, TargetIterator, TestConfig
from perfrunner.tests import PerfTest
from perfrunner.tests.ycsb import YCSBTest


class OperatorTest(PerfTest):

    COLLECTORS = {
        'ns_server_system': True
    }

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.reporter.build = f"{self.remote.get_operator_version()}:{self.build}"


class OperatorBackupTest(OperatorTest):

    @with_stats
    def backup(self):
        logger.info('Running backup')
        self.remote.create_backup()
        self.remote.wait_for_backup_complete()
        logger.info('Backup complete')

    def parse_backup_status(self, backup_status):
        start_time = backup_status['lastRun']
        end_time = backup_status['lastSuccess']
        start_dt = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
        time_elapsed = end_dt - start_dt
        time_elapsed = time_elapsed.total_seconds()
        capacity_used = backup_status['capacityUsed']
        if "Mi" in capacity_used:
            backup_size = round(float(backup_status['capacityUsed'].strip("Mi"))/1024, 2)
        if "Gi" in capacity_used:
            backup_size = round(float(backup_status['capacityUsed'].strip("Gi")), 2)
        return time_elapsed, backup_size

    def _report_kpi(self, time_elapsed, backup_size):
        edition = 'Operator'
        tool = 'backup'
        storage = None
        self.reporter.post(
            *self.metrics.bnr_throughput(
                time_elapsed,
                edition,
                tool,
                storage)
        )
        self.reporter.post(
            *self.metrics.backup_size(
                backup_size,
                edition,
                tool,
                storage)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.backup()
        backup_status = self.remote.get_backup('my-backup')['status']
        time_elapsed, backup_size = self.parse_backup_status(backup_status)
        self.report_kpi(time_elapsed, backup_size)


class OperatorBackupRestoreTest(OperatorBackupTest):

    @with_stats
    @timeit
    def restore(self):
        logger.info('Running restore')
        self.remote.create_restore()
        self.check_num_items(max_retry=3600)

    def flush_buckets(self):
        logger.info('Flushing bucket')
        for i in range(self.test_config.cluster.num_buckets):
            bucket = 'bucket-{}'.format(i + 1)
            self.rest.flush_bucket(self.master_node, bucket)
            self.check_num_items(bucket_items={bucket: 0})

    def _report_kpi(self, time_elapsed_backup, time_elapsed_restore, backup_size):
        edition = 'Operator'
        storage = None
        tool = 'backup'

        self.reporter.post(
            *self.metrics.bnr_throughput(
                time_elapsed_backup,
                edition,
                tool,
                storage)
        )

        self.reporter.post(
            *self.metrics.backup_size(
                backup_size,
                edition,
                tool,
                storage)
        )

        tool = 'restore'
        self.reporter.post(
            *self.metrics.bnr_throughput(
                time_elapsed_restore,
                edition,
                tool,
                storage)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.backup()
        backup_status = self.remote.get_backup('my-backup')['status']
        time_elapsed_backup, backup_size = self.parse_backup_status(backup_status)
        self.flush_buckets()
        time_elapsed_restore = self.restore()
        self.report_kpi(time_elapsed_backup, time_elapsed_restore, backup_size)


class OperatorBackupRestoreYCSBTest(YCSBTest, OperatorBackupRestoreTest):

    def run(self):
        self.download_ycsb()
        self.load()
        time.sleep(30)
        self.wait_for_persistence()
        self.check_num_items()
        self.backup()
        backup_status = self.remote.get_backup('my-backup')['status']
        time_elapsed_backup, backup_size = self.parse_backup_status(backup_status)
        self.flush_buckets()
        time_elapsed_restore = self.restore()
        self.report_kpi(time_elapsed_backup, time_elapsed_restore, backup_size)


class OperatorUpgradeTest(YCSBTest):
    def delay(self):
        # Delay to wait for the next reconcilliation loop
        time.sleep(60)

    def k8s_cordon_drain(self) -> bool:
        active_server_nodes = self.remote.get_cb_cluster_pod_nodes()
        all_server_nodes = self.remote.get_all_server_nodes()
        # Nodes designated for server pods but currently dont have a running pod
        empty_nodes = list(set(all_server_nodes.keys()).difference(active_server_nodes))
        if not empty_nodes:
            logger.warn("No free node is available to perform cordon/drain")
            return False

        # Need the node to be drained coming from the same AZ as the free node.
        # This way the CSI driver can access the attached PVC to perform recovery.
        logger.info(f"Server nodes: {all_server_nodes}")
        logger.info(f"Empty nodes: {empty_nodes}")
        used_empty_node = empty_nodes[0]
        available_az = all_server_nodes.get(used_empty_node)
        target_server_node = None
        for node in active_server_nodes:
            if all_server_nodes.get(node) == available_az and node not in self.processed_nodes:
                target_server_node = node
                self.processed_nodes.append(node)
                if used_empty_node not in self.processed_nodes:
                    self.processed_nodes.append(used_empty_node)
                break

        if not target_server_node:
            return False

        logger.info(f"Simulating k8s upgrade using node {target_server_node} on az {available_az}")
        self.remote.cordon_a_node(target_server_node)
        self.delay()
        self.remote.drain_a_node(target_server_node)
        self.delay()  # Give time for the operator to react to a pod eviction
        self.remote.wait_for_cluster_ready(timeout=7200)
        self.delay()
        self.remote.uncordon_a_node(target_server_node)
        return True

    @timeit
    def upgrade(self):
        upgrade_settings = self.test_config.upgrade_settings

        logger.info(f"Pods before: \n{self.remote.get_pods(output='wide')}")
        if upgrade_settings.is_server_upgrade():
            if not upgrade_settings.target_version:
                logger.warn("No target version specified. No upgrade will be performed.")
                return

            logger.info(
                f"Upgrading server from {self.remote.get_current_server_version()} "
                f"to {upgrade_settings.target_version}"
            )
            self.remote.upgrade_couchbase_server(upgrade_settings.target_version)
            self.monitor.monitor_server_upgrade()
        else:
            # Cordon/drain simulation
            self.processed_nodes = []
            # Simulate individual upgrade of all possible nodes one by one
            while self.k8s_cordon_drain():
                self.delay()
                logger.info(f"Current pods state: \n{self.remote.get_pods(output='wide')}")

        logger.info(f"Pods after: \n{self.remote.get_pods(output='wide')}")

    def run(self):
        if self.test_config.access_settings.ssl_mode == "data":
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.create_indexes()
        self.wait_for_indexing()

        try:
            self.load()
        except Exception as e:
            logger.error(f"Load failed: {e}")

        self.wait_for_indexing()
        self.access_bg()

        time.sleep(self.test_config.upgrade_settings.start_after)
        upgrade_time = self.upgrade()
        logger.info(f"Upgrade took {round(upgrade_time/ 60, 1)} min")

        self.worker_manager.wait_for_bg_tasks()

        self.collect_export_files()
        self.report_kpi(upgrade_time)

    def _report_kpi(self, upgrade_time: float):
        self.reporter.post(*self.metrics.elapsed_time(upgrade_time))

    def create_indexes(self):
        for statement_template in self.test_config.index_settings.statements:
            for bucket in self.test_config.buckets:
                for indx in range(self.test_config.index_settings.indexes_per_collection):
                    statement = statement_template.format(indx, bucket)
                    logger.info("Creating index: " + statement)
                    self.rest.exec_n1ql_statement(self.query_nodes[0], statement)


class CNGOperatorTest(YCSBTest):

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        svc_host = self._get_service_host()
        logger.info(f"Using service address: {svc_host}")
        self.target_iterator = TargetIterator(cluster_spec, test_config, target_svc=svc_host)

    def _get_service_host(self):
        svc_host = "cb-example-perf"
        scheme = "couchbase2" if self.test_config.cng else "couchbases"
        if self.cluster_spec.external_client:
            svc_host = f"{scheme}://{self.remote.get_external_service_dns()}"

        return svc_host


class CNGThroughputTest (CNGOperatorTest):
    def _report_kpi(self):
        self.collect_export_files()

        self.reporter.post(
            *self.metrics.ycsb_throughput()
        )


class ClusterMigrationTest(OperatorTest):
    """Test that measures the performance of migrating a cluster from self-managed to Kubernetes.

    The test follows these steps:
    1. Loads initial data into a self-managed cluster
    2. Starts a background access workload against the self-managed cluster
    3. Deploys and configures a new Kubernetes cluster to migrate the self-managed cluster
    4. Migrates data to the K8s cluster while access workload continues
    5. Reports KPIs:
       - Time taken to deploy K8s cluster
       - Latency percentiles for set and durable_set operations during migration
    """

    ALL_HOSTNAMES = True

    COLLECTORS = {"ns_server_system": True, "latency": True}

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        # Skip loading operator version from OperatorTest, as we start from an self-managed cluster
        PerfTest.__init__(self, cluster_spec, test_config, verbose)
        # Workaround for mixed mode clusters, the default remote is a self-managed cluster
        self.remote_k8s = RemoteKubernetes(self.cluster_spec)
        self.reporter.build = f"{self.remote_k8s.get_operator_version()}:{self.build}"
        self.migration_time = 0

    @timeit
    def _migrate(self, source_cluster: str, timeout: int):
        logger.info(f"Migrating from source {source_cluster}")
        cm = KubernetesClusterManager(self.cluster_spec, self.test_config)
        cm.configure_cluster()
        cm.deploy_couchbase_cluster(timeout=timeout)

    def migrate(self):
        migration_settings = self.test_config.migration_settings
        logger.info(f"Waiting {migration_settings.start_after}s before starting cluster migration")
        time.sleep(migration_settings.start_after)

        self.migration_time = self._migrate(
            migration_settings.source_cluster, migration_settings.migration_timeout_seconds
        )
        logger.info(f"Migration took {round(self.migration_time / 60, 1)} min")

        logger.info(
            f"Cluster migration completed. Waiting {migration_settings.stop_after}s before "
            "continuing"
        )
        time.sleep(migration_settings.stop_after)
        # At this point the access phase workload should be going to the k8s cluster
        self.worker_manager.abort_all_tasks()

    def _report_kpi(self, *args):
        with TimeTrackingFile() as t:
            k8s_deployment_time = t.get("k8s_cluster_deployment")

        self.reporter.post(
            *self.metrics.cluster_deployment_time(
                k8s_deployment_time, "k8s", "Migration Time (sec)"
            )
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        # The self-managed cluster doesnt exist anymore after migration.
        # So running anything against it will fail.
        pass

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        # Start access workload on a self-managed cluster
        self.access_bg()
        self.migrate()

        self.report_kpi()
