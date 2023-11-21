import json
import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.helpers.profiler import with_profiles
from perfrunner.tests import PerfTest
from perfrunner.tests.analytics import BigFunTest
from perfrunner.tests.eventing import EventingTest
from perfrunner.tests.fts import FTSTest
from perfrunner.tests.n1ql import N1QLTest
from perfrunner.tests.rebalance import RebalanceKVTest
from perfrunner.tests.tools import BackupRestoreTest
from perfrunner.tests.xdcr import SrcTargetIterator, UniDirXdcrInitTest


class HighBucketDensityTest(RebalanceKVTest,
                            UniDirXdcrInitTest,
                            N1QLTest,
                            EventingTest,
                            BigFunTest,
                            FTSTest,
                            BackupRestoreTest):

    COLLECTORS = {
        'iostat': True,
        'memory': True,
        'n1ql_stats': True,
        'secondary_stats': True,
        'ns_server_system': True,
        'xdcr_stats': True,
        'analytics': True
    }
    SLEEP_TIME_BETWEEN_REBALNCE = 600

    def rebalance(self, *args):
        self.pre_rebalance()
        rebalance_time = self._rebalance(None)
        self.post_rebalance()
        logger.info("Rebalance time: {} min".format(rebalance_time / 60))

    def access_and_rebalance(self, src_target_iterator: SrcTargetIterator):

        time.sleep(self.SLEEP_TIME_BETWEEN_REBALNCE)
        self.store_plans()

        src_target_iterator_access = SrcTargetIterator(self.cluster_spec,
                                                       self.test_config,
                                                       "access")
        PerfTest.access_bg(self, target_iterator=src_target_iterator_access)
        self.access_n1ql_bg()

        access_settings = self.test_config.access_settings
        access_settings.spring_batch_size = 5
        access_settings.creates = 0
        access_settings.updates = 1
        access_settings.reads = 3
        access_settings.deletes = 1
        access_settings.throughput = 5
        PerfTest.access_bg(self, settings=access_settings,
                           target_iterator=src_target_iterator)

        self.rebalance()

    def add_eventing_functions(self):
        with open(self.config_file) as f:
            func = json.load(f)
        func["settings"]["dcp_stream_boundary"] = "from_now"

        """
        Commenting out as second function deployment is failing MB-32437
        for bkt in self.test_config.buckets:
            func["depcfg"]["source_bucket"] = bkt
            func["appname"] = bkt + "-test1"
            self.set_functions_with_config(func=func, override_name=False, wait_for_bootstrap=True)
        """
        func["depcfg"]["source_bucket"] = "bucket-1"
        func["appname"] = "bucket-1-test1"
        self.set_functions_with_config(func=func, override_name=False, wait_for_bootstrap=True)

    @with_stats
    @with_profiles
    def init_only_xdcr(self):
        self.add_remote_cluster()
        self.create_replication()
        time.sleep(3600)

    def create_analytics_datasets(self):
        logger.info('Creating datasets')

        for bkt in self.test_config.buckets:
            statement = "CREATE DATASET `{}` ON `{}` WHERE category > 1;"\
                .format(bkt + "-ds1", bkt)
            logger.info(statement)
            self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                               statement)

            statement = "CREATE DATASET `{}` ON `{}` WHERE category < 1;" \
                .format(bkt + "-ds2", bkt)
            logger.info(statement)
            self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                               statement)
        self.connect_buckets()

    def create_fts_indexes(self):
        less_words = True
        for bkt in self.test_config.buckets:
            definition = read_json(self.jts_access.couchbase_index_configfile)
            if less_words:
                name = "fts_less_words"
                less_words = False
            else:
                name = "fts_more_words"
                less_words = True
            index_name = bkt + "-" + name
            definition.update({
                'name': index_name,
                'sourceName': bkt,
            })
            mapping = definition["params"]["mapping"]["default_mapping"]

            prop = definition["params"]["mapping"]["default_mapping"]["properties"]
            index = prop["fts_less_words"]
            new_prop = {name: index}
            mapping.update({
                'properties': new_prop
            })

            index = \
                definition["params"]["mapping"]["default_mapping"]["properties"][name]["fields"][0]
            index.update({
                'name': name
            })

            logger.info('Index definition: {}'.format(pretty_dict(definition)))
            self.rest.create_fts_index(self.fts_master_node,
                                       index_name, definition)
            self.monitor.monitor_fts_indexing_queue(self.fts_nodes[0],
                                                    index_name,
                                                    int(self.test_config.access_settings.items *
                                                        0.95))
            self.monitor.monitor_fts_index_persistence(self.fts_nodes,
                                                       index_name, bkt)

    @timeit
    def custom_rebalance(self, services, initial_nodes, nodes_after, swap):
        clusters = self.cluster_spec.clusters

        for (_, servers), initial_nodes, nodes_after in zip(clusters,
                                                            initial_nodes,
                                                            nodes_after):
            master = servers[0]

            new_nodes = []
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []

            if nodes_after > initial_nodes:  # rebalance-in
                new_nodes = servers[initial_nodes:nodes_after]
                known_nodes = servers[:nodes_after]
            elif nodes_after < initial_nodes:  # rebalance-out
                ejected_nodes = servers[nodes_after:initial_nodes]
                logger.info("ejected_nodes {}".format(ejected_nodes))
            elif swap:
                service = services.split(",")[0]
                new_nodes = servers[initial_nodes:initial_nodes + swap]
                known_nodes = servers[:initial_nodes + swap]
                ejected_nodes = [self.cluster_spec.servers_by_role_from_first_cluster(service)[-1]]
                if ejected_nodes[0] == new_nodes[0]:
                    ejected_nodes = \
                        [self.cluster_spec.servers_by_role_from_first_cluster(service)[-2]]
                logger.info("ejected_nodes {}".format(ejected_nodes))
            else:
                continue

            for node in new_nodes:
                logger.info("Adding {} as {}".format(node, services))
                self.rest.add_node(master, node, services=services)

            self.rest.rebalance(master, known_nodes, ejected_nodes)
            logger.info("Rebalance master: {}".format(master))

            self.monitor_progress(master)

            if swap:
                time.sleep(self.SLEEP_TIME_BETWEEN_REBALNCE)
                for node in ejected_nodes:
                    logger.info("Adding {} as {}".format(node, services))
                    self.rest.add_node(master, node, services=services)

                logger.info("ejected_nodes {}".format(servers[initial_nodes:initial_nodes + 1]))
                self.rest.rebalance(master, known_nodes, servers[initial_nodes:initial_nodes + 1])
                logger.info("Rebalance master: {}".format(master))

                self.monitor_progress(master)

            break

    def rebalance_out_node(self, services):
        logger.info("Rebalancing out {} node".format(services))
        rebalance_time = self.custom_rebalance(services,
                                               self.rebalance_settings.nodes_after,
                                               self.test_config.cluster.initial_nodes,
                                               0)
        logger.info("Rebalancing out {} node, COMPLETED.".format(services))
        logger.info("Rebalance time: {} min".format(rebalance_time / 60))
        time.sleep(self.SLEEP_TIME_BETWEEN_REBALNCE)

    def rebalance_in_node(self, services):
        logger.info("Rebalancing in {} node, STARTING".format(services))
        rebalance_time = self.custom_rebalance(services,
                                               self.test_config.cluster.initial_nodes,
                                               self.rebalance_settings.nodes_after,
                                               0)
        logger.info("Rebalancing in {} node, COMPLETED.".format(services))
        logger.info("Rebalance time: {} min".format(rebalance_time / 60))
        time.sleep(self.SLEEP_TIME_BETWEEN_REBALNCE)

    def swap_node(self, services):
        logger.info("Swapping in {} node, STARTING".format(services))
        rebalance_time = self.custom_rebalance(services,
                                               self.test_config.cluster.initial_nodes,
                                               self.test_config.cluster.initial_nodes,
                                               1)
        logger.info("Swapping in {} node, COMPLETED.".format(services))
        logger.info("Rebalance time: {} min".format(rebalance_time / 60))
        time.sleep(self.SLEEP_TIME_BETWEEN_REBALNCE)

    @with_stats
    def run_kv_rebalance(self):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config,
                                                "initial")
        self.access_and_rebalance(src_target_iterator)
        self.rebalance_out_node("kv")
        self.swap_node("kv")

    @with_stats
    def run_index_rebalance(self):
        self.rebalance_in_node("index")
        self.rebalance_out_node("index")
        self.swap_node("index,n1ql")

    @with_stats
    def run_eventing_rebalance(self):
        self.rebalance_in_node("eventing")
        self.rebalance_out_node("eventing")
        self.swap_node("eventing")

    @with_stats
    def run_fts_rebalance(self):
        self.rebalance_in_node("fts")
        self.rebalance_out_node("fts")
        self.swap_node("fts")

    @with_stats
    def run_cbas_rebalance(self):
        self.rebalance_in_node("cbas")
        self.rebalance_out_node("cbas")
        self.swap_node("cbas")

    def run_rebalance_phase(self):
        self.run_kv_rebalance()
        self.run_index_rebalance()
        self.run_eventing_rebalance()
        self.run_cbas_rebalance()
        """
        Commenting FTS rebalance phase as swap rebalance for FTS always fails - MB-32547
        Pushing it as last step in test, we shall uncomment this once above bug is fixed.
        self.run_fts_rebalance()
        """

    @with_stats
    @timeit
    def backup(self, mode=None):
        super().backup(mode)

    def back_up(self):
        self.extract_tools()
        time_elapsed = self.backup()
        logger.info("Backup time: {} min".format(time_elapsed / 60))

    def run(self):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config,
                                                "initial")
        PerfTest.load(self, target_iterator=src_target_iterator)

        self.wait_for_persistence()

        t0 = time.time()
        self.create_indexes()
        self.wait_for_indexing()
        index_build_time = time.time() - t0
        logger.info("Index build time: {} min".format(index_build_time / 60))

        self.init_only_xdcr()
        """
        Commenting out wait for xdcr init as it is very slow
        time_elapsed = self.init_xdcr()
        logger.info("XDCR init time: {} min".format(time_elapsed / 60))
        """

        self.add_eventing_functions()

        self.create_fts_indexes()

        self.create_analytics_datasets()

        self.run_rebalance_phase()

        self.back_up()

        """
        Keeping this at end as FTS swap rebalance fails - MB-32547
        """
        self.run_fts_rebalance()


class MultibucketEventing(HighBucketDensityTest):
    COLLECTORS = {
        'iostat': True,
        'memory': True,
        'ns_server_system': True}

    def add_eventing_functions(self):
        with open(self.config_file) as f:
            func = json.load(f)
        func["settings"]["dcp_stream_boundary"] = "from_now"

        """
        Commenting out as second function deployment is failing MB-32437
        """
        for bkt in self.test_config.buckets:
            func["depcfg"]["source_bucket"] = bkt
            func["appname"] = bkt + "-test1"
            self.set_functions_with_config(func=func, override_name=False, wait_for_bootstrap=True)

    def access_and_rebalance(self, src_target_iterator: SrcTargetIterator):

        time.sleep(self.SLEEP_TIME_BETWEEN_REBALNCE)

        src_target_iterator_access = SrcTargetIterator(self.cluster_spec,
                                                       self.test_config,
                                                       "access")
        PerfTest.access_bg(self, target_iterator=src_target_iterator_access)

        access_settings = self.test_config.access_settings
        access_settings.spring_batch_size = 5
        access_settings.creates = 0
        access_settings.updates = 1
        access_settings.reads = 3
        access_settings.deletes = 1
        access_settings.throughput = 5
        PerfTest.access_bg(self, settings=access_settings,
                           target_iterator=src_target_iterator)

        self.rebalance()

    @with_stats
    def run_kv_rebalance(self):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config,
                                                "initial")
        self.access_and_rebalance(src_target_iterator)
        self.rebalance_out_node("kv")
        self.swap_node("kv")

    def run(self):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config,
                                                "initial")
        PerfTest.load(self, target_iterator=src_target_iterator)

        self.wait_for_persistence()
        self.add_eventing_functions()
        self.run_kv_rebalance()
        self.run_eventing_rebalance()


class MultibucketGSI(HighBucketDensityTest):
    COLLECTORS = {
        'iostat': True,
        'memory': True,
        'ns_server_system': True,
        'secondary_stats': True}

    # This is to verify the bug : MB-39144

    def run(self):
        # PerfTest.load(self, target_iterator=src_target_iterator)
        # self.wait_for_persistence()
        t0 = time.time()
        PerfTest.create_indexes(self)
        self.wait_for_indexing()
        index_build_time = time.time() - t0
        logger.info("Index build time: {} min".format(index_build_time / 60))
