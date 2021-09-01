import calendar
import copy
import json
import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import (
    pillowfight_data_load_task,
    pillowfight_task,
)
from perfrunner.settings import TargetSettings
from perfrunner.tests import PerfTest, TargetIterator


class EventingTargetIterator(TargetIterator):

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        src_master = next(self.cluster_spec.masters)
        for bucket in self.test_config.buckets:
            if self.prefix == "None":
                yield TargetSettings(src_master, bucket, password, None)
            else:
                yield TargetSettings(src_master, bucket, password, prefix)


class EventingDestBktTargetIterator(TargetIterator):

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        src_master = next(self.cluster_spec.masters)
        for bucket in self.test_config.eventing_buckets:
            if self.prefix == "None":
                yield TargetSettings(src_master, bucket, password, None)
            else:
                yield TargetSettings(src_master, bucket, password, prefix)


class EventingTest(PerfTest):

    """Eventing test base class.

    This is base class for eventing related operations required
    to measure eventing performance parameters.
    """

    COLLECTORS = {'eventing_stats': True, 'ns_server_system': True}

    def __init__(self, *args):
        super().__init__(*args)

        self.functions = self.test_config.eventing_settings.functions
        self.worker_count = self.test_config.eventing_settings.worker_count
        self.cpp_worker_thread_count = self.test_config.eventing_settings.cpp_worker_thread_count
        self.timer_worker_pool_size = self.test_config.eventing_settings.timer_worker_pool_size
        self.worker_queue_cap = self.test_config.eventing_settings.worker_queue_cap
        self.timer_timeout = self.test_config.eventing_settings.timer_timeout
        self.timer_fuzz = self.test_config.eventing_settings.timer_fuzz
        self.config_file = self.test_config.eventing_settings.config_file
        self.time = self.test_config.access_settings.time
        self.rebalance_settings = self.test_config.rebalance_settings
        self.request_url = self.test_config.eventing_settings.request_url
        self.key_prefix = self.test_config.load_settings.key_prefix or "eventing"
        self.dest_bkt_doc_gen = self.test_config.eventing_settings.eventing_dest_bkt_doc_gen

        if self.functions == {}:
            with open(self.config_file) as f:
                funcs = json.load(f)
                for func_settings in funcs:
                    self.functions[func_settings["appname"]] = func_settings["code_path"]

        for master in self.cluster_spec.masters:
            self.rest.add_rbac_user(
                host=master,
                user="eventing",
                password="password",
                roles=['admin'],
            )

        self.target_iterator = EventingTargetIterator(self.cluster_spec,
                                                      self.test_config,
                                                      self.key_prefix)
        if self.test_config.access_settings.ssl_mode == 'n2n' or \
                self.test_config.load_settings.ssl_mode == 'n2n':
            self.download_certificate()

    @timeit
    def deploy_and_bootstrap(self, func, name, wait_for_bootstrap):
        self.rest.deploy_function(node=self.eventing_nodes[0],
                                  func=func, name=name)
        if wait_for_bootstrap:
            self.monitor.wait_for_bootstrap(nodes=self.eventing_nodes,
                                            function=name)

    def set_functions(self) -> float:
        with open(self.config_file) as f:
            func = json.load(f)

        return self.set_functions_with_config(func=func)

    def set_functions_with_config(self, func,
                                  wait_for_bootstrap: bool = True):
        time_to_deploy = 0
        for func_settings in func:

            func_settings["settings"]["worker_count"] = self.worker_count
            func_settings["settings"]["cpp_worker_thread_count"] = self.cpp_worker_thread_count
            func_settings["settings"]["timer_worker_pool_size"] = self.timer_worker_pool_size
            func_settings["settings"]["worker_queue_cap"] = self.worker_queue_cap

            if "curl" in func_settings["depcfg"]:
                func_settings["depcfg"]["curl"][0]["hostname"] = self.request_url

            code_file = self.functions[func_settings["appname"]]

            with open(code_file, 'r') as myfile:
                code = myfile.read()
                if self.timer_timeout:
                    expiry = (calendar.timegm(time.gmtime()) + self.timer_timeout) * 1000
                    code = code.replace("fixed_expiry", str(expiry))
                    code = code.replace("fuzz_factor", str(self.timer_fuzz))
                func_settings["appcode"] = code
            temp_time_to_deploy = self.deploy_and_bootstrap(func_settings, func_settings["appname"],
                                                            wait_for_bootstrap)
            logger.info("Function {} deployed, time taken for deployment {}".
                        format(func_settings["appname"], temp_time_to_deploy))
            time_to_deploy += temp_time_to_deploy
        return time_to_deploy

    def process_latency_stats(self):
        ret_val = {}
        all_stats = self.rest.get_eventing_stats(node=self.eventing_nodes[0], full_stats=True)
        for stat in all_stats:
            latency_stats = stat["latency_stats"]
            ret_val[stat["function_name"]] = sorted(latency_stats.items(), key=lambda x: int(x[0]))

            if "curl_latency_stats" in stat:
                curl_latency_stats = stat["curl_latency_stats"]
            else:
                curl_latency_stats = {}
            ret_val["curl_latency_" + stat["function_name"]] = \
                sorted(curl_latency_stats.items(), key=lambda x: int(x[0]))

        return ret_val

    def get_on_update_success(self):
        on_update_success = 0
        for node in self.eventing_nodes:
            stats = self.rest.get_eventing_stats(node=node)
            for stat in stats:
                logger.info("Execution stats for {node}: {stats}"
                            .format(node=node,
                                    stats=pretty_dict(stat["execution_stats"])))
                on_update_success += stat["execution_stats"]["on_update_success"]
        return on_update_success

    def get_timer_responses(self):
        doc_timer_responses = 0
        for node in self.eventing_nodes:
            stats = self.rest.get_eventing_stats(node=node)
            for stat in stats:
                logger.info("Event processing stats for {node}: {stats}"
                            .format(node=node,
                                    stats=pretty_dict(stat["event_processing_stats"])))
                doc_timer_responses += \
                    stat["event_processing_stats"]["timer_responses_received"]
        return doc_timer_responses

    def get_timer_msg_counter(self):
        timer_msg_counter = 0
        for node in self.eventing_nodes:
            stats = self.rest.get_eventing_stats(node=node)
            for stat in stats:
                logger.info("Execution stats for {node}: {stats}"
                            .format(node=node,
                                    stats=pretty_dict(stat["execution_stats"])))
                timer_msg_counter += stat["execution_stats"]["timer_msg_counter"]
        return timer_msg_counter

    @with_stats
    @with_profiles
    @timeit
    def load_access_and_wait(self):
        self.load()
        self.access_bg()
        self.sleep()

    @timeit
    def undeploy_function(self, name):
        func = '{"processing_status":false, "deployment_status":false}'
        self.rest.change_function_settings(node=self.eventing_nodes[0],
                                           func=func, name=name)
        self.monitor.wait_for_function_status(node=self.eventing_nodes[0], function=name,
                                              status="undeployed")

    @timeit
    def pause_function(self, name):
        func = '{"processing_status":false, "deployment_status":true}'
        self.rest.change_function_settings(node=self.eventing_nodes[0],
                                           func=func, name=name)
        self.monitor.wait_for_function_status(node=self.eventing_nodes[0], function=name,
                                              status="paused")

    @timeit
    def resume_function(self, name):
        func = '{"processing_status":true, "deployment_status":true}'
        self.rest.change_function_settings(node=self.eventing_nodes[0],
                                           func=func, name=name)
        self.monitor.wait_for_function_status(node=self.eventing_nodes[0], function=name,
                                              status="deployed")

    def undeploy(self) -> int:
        time_to_undeploy = 0
        for name, filename in self.functions.items():
            time_to_undeploy += self.undeploy_function(name=name)
            logger.info("Function {} is undeployed.".format(name))
        return time_to_undeploy

    def pause(self) -> int:
        time_to_pause = 0
        for name, filename in self.functions.items():
            time_to_pause += self.pause_function(name=name)
            logger.info("Function {} is paused.".format(name))
        return time_to_pause

    def resume(self) -> int:
        time_to_resume = 0
        for name, filename in self.functions.items():
            time_to_resume += self.resume_function(name=name)
            logger.info("Function {} is resumed.".format(name))
        return time_to_resume

    def validate_failures(self):
        ignore_failures = ["uv_try_write_failure_counter",
                           "bucket_op_exception_count", "timestamp",
                           "bkt_ops_cas_mismatch_count", "bucket_op_cache_miss_count"]
        for node in self.eventing_nodes:
            all_stats = self.rest.get_eventing_stats(node=node)

            logger.info("Stats for {node} : {stats}"
                        .format(node=node,
                                stats=pretty_dict(all_stats)))
            for function_stats in all_stats:
                execution_stats = function_stats["execution_stats"]
                failure_stats = function_stats["failure_stats"]

                # Validate Execution stats
                for stat, value in execution_stats.items():
                    if "failure" in stat and value != 0 and stat not in ignore_failures:
                        raise Exception(
                            '{function}: {node}: {stat} is not zero'.format(
                                function=function_stats["function_name"], node=node, stat=stat))

                # Validate Failure stats
                for stat, value in failure_stats.items():
                    if value != 0 and stat not in ignore_failures:
                        raise Exception(
                            '{function}: {node}: {stat} is not zero'.format(
                                function=function_stats["function_name"], node=node, stat=stat))

    def print_max_rss_values(self):
        for node in self.eventing_nodes:
            for name, filename in self.functions.items():
                try:
                    max_consumer_rss, max_producer_rss = \
                        self.metrics.get_max_rss_values(function_name=name, server=node)
                    logger.info("Max Consumer rss is {}MB on {} for function {}".
                                format(max_consumer_rss, node, name))
                    logger.info("Max Producer rss is {}MB on {} for function {}".
                                format(max_producer_rss, node, name))
                except (ValueError, IndexError):
                    logger.info("Failed to get max rss on {}".format(node))

    def debug(self):
        self.print_max_rss_values()
        self.validate_failures()
        return super().debug()

    def run(self):
        self.set_functions()

        time_elapsed = self.load_access_and_wait()

        self.report_kpi(time_elapsed)


class FunctionsTimeTest(EventingTest):
    @timeit
    def process_all_docs(self):
        self.monitor.wait_for_all_mutations_processed(host=self.master_node,
                                                      bucket1=self.test_config.buckets[0],
                                                      bucket2=self.test_config.eventing_buckets[0]
                                                      )

    @with_stats
    def apply_function(self):
        time_to_deploy = self.set_functions()
        time_to_process = self.process_all_docs()
        return time_to_deploy, time_to_deploy + time_to_process

    def run(self):
        self.load()

        time_to_deploy, time_to_process = self.apply_function()

        self.report_kpi(time_to_deploy, time_to_process)

    def _report_kpi(self, time_to_deploy, time_to_process):
        self.reporter.post(
            *self.metrics.function_time(time=time_to_deploy,
                                        time_type="bootstrap",
                                        initials="Bootstrap time(min)")
        )
        self.reporter.post(
            *self.metrics.function_time(time=time_to_process,
                                        time_type="processing",
                                        initials="Processing time(min)")
        )


class FunctionsPhaseChangeTimeTest(EventingTest):
    TIME_BETWEEN_PHASES = 300

    @with_stats
    def apply_function(self):
        time_to_deploy = self.set_functions()
        self.access_bg()
        time.sleep(self.TIME_BETWEEN_PHASES)
        time_to_pause = self.pause()
        time.sleep(self.TIME_BETWEEN_PHASES)
        time_to_resume = self.resume()
        time.sleep(self.TIME_BETWEEN_PHASES)
        time_to_undeploy = self.undeploy()
        return time_to_deploy, time_to_pause, time_to_resume, time_to_undeploy

    def run(self):
        self.load()

        time_to_deploy, time_to_pause, time_to_resume, time_to_undeploy = self.apply_function()

        self.report_kpi(time_to_deploy, time_to_pause, time_to_resume, time_to_undeploy)

    def _report_kpi(self, time_to_deploy, time_to_pause, time_to_resume, time_to_undeploy):
        self.reporter.post(
            *self.metrics.function_time(time=round(time_to_deploy, 1),
                                        time_type="deploy",
                                        initials="Deploy time(sec)",
                                        unit="sec")
        )
        self.reporter.post(
            *self.metrics.function_time(time=round(time_to_pause, 1),
                                        time_type="pause",
                                        initials="Pause time(sec)",
                                        unit="sec")
        )
        self.reporter.post(
            *self.metrics.function_time(time=round(time_to_resume, 1),
                                        time_type="resume",
                                        initials="Resume time(sec)",
                                        unit="sec")
        )
        self.reporter.post(
            *self.metrics.function_time(time=round(time_to_undeploy, 1),
                                        time_type="undeploy",
                                        initials="Undeploy time(sec)",
                                        unit="sec")
        )


class FunctionsThroughputTest(EventingTest):
    def _report_kpi(self, time_elapsed):
        events_successfully_processed = self.get_on_update_success()
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=events_successfully_processed)
        )


class FunctionsThroughputTestDestBucket(FunctionsThroughputTest):

    def load_dest_bucket(self):
        target_iterator = EventingDestBktTargetIterator(self.cluster_spec,
                                                        self.test_config,
                                                        self.key_prefix)

        load_settings = copy.deepcopy(self.test_config.load_settings)
        load_settings.doc_gen = self.dest_bkt_doc_gen
        self.load(settings=load_settings, target_iterator=target_iterator)

    def run(self):
        self.set_functions()

        self.load_dest_bucket()

        time_elapsed = self.load_access_and_wait()

        self.report_kpi(time_elapsed)


class FunctionsPillowfightThroughputTest(FunctionsThroughputTest):

    ALL_BUCKETS = True

    def load(self, *args):
        PerfTest.load(self, task=pillowfight_data_load_task)

    def access_bg(self, *args):
        self.download_certificate()

        PerfTest.access_bg(self, task=pillowfight_task)


class CreateTimerThroughputTest(EventingTest):
    def _report_kpi(self, time_elapsed):
        events_successfully_processed = self.get_timer_responses()
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=events_successfully_processed)
        )


class FunctionsIndexThroughputTest(EventingTest):
    def __init__(self, *args):
        super().__init__(*args)
        self.on_update_success = 0

    @with_stats
    @timeit
    def access_and_wait(self):
        self.create_index()
        self.access_bg()
        self.sleep()

    def create_index(self):
        storage = self.test_config.gsi_settings.storage
        indexes = self.test_config.gsi_settings.indexes

        if self.test_config.collection.collection_map:
            for server in self.index_nodes:
                for bucket, scope_map in indexes.items():
                    for scope, collection_map in scope_map.items():
                        for collection, index_map in collection_map.items():
                            for name, field in index_map.items():
                                self.rest.create_index(host=server,
                                                       bucket=bucket,
                                                       scope=scope,
                                                       collection=collection,
                                                       name=name,
                                                       field=field,
                                                       storage=storage)

        else:
            for server in self.index_nodes:
                for bucket in self.test_config.buckets:
                    for name, field in indexes.items():
                        self.rest.create_index(host=server,
                                               bucket=bucket,
                                               name=name,
                                               field=field,
                                               storage=storage)

    def run(self):
        self.set_functions()

        self.load()

        on_update_success = self.get_on_update_success()
        time_elapsed = self.access_and_wait()
        self.on_update_success = \
            self.get_on_update_success() - on_update_success

        self.report_kpi(time_elapsed)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=self.on_update_success)
        )


class FunctionsThroughputIndexN1QLTest(FunctionsIndexThroughputTest):
    def run(self):
        self.set_functions()

        self.create_index()

        time_elapsed = self.load_access_and_wait()

        self.report_kpi(time_elapsed)

    def _report_kpi(self, time_elapsed):
        events_successfully_processed = self.get_on_update_success()
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=events_successfully_processed)
        )


class EventingRebalance(EventingTest):

    def pre_rebalance(self):
        """Execute additional steps before rebalance."""
        logger.info('Sleeping for {} seconds before taking actions'
                    .format(self.rebalance_settings.start_after))
        time.sleep(self.rebalance_settings.start_after)

    def post_rebalance(self):
        """Execute additional steps after rebalance."""
        logger.info('Sleeping for {} seconds before finishing'
                    .format(self.rebalance_settings.stop_after))
        time.sleep(self.rebalance_settings.stop_after)

    def rebalance(self, initial_nodes, nodes_after):
        for _, servers in self.cluster_spec.clusters:
            master = servers[0]
            ejected_nodes = []
            new_nodes = enumerate(
                servers[initial_nodes:nodes_after],
                start=initial_nodes
            )
            known_nodes = servers[:nodes_after]
            for i, node in new_nodes:
                roles = self.cluster_spec.roles[node]
                self.rest.add_node(master, node, roles)
                if "eventing" in roles:
                    self.eventing_nodes.append(node)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

    @timeit
    def rebalance_time(self):
        initial_nodes = self.test_config.cluster.initial_nodes
        self.rebalance(initial_nodes[0], self.rebalance_settings.nodes_after[0])
        self.monitor.monitor_rebalance(self.master_node)


class FunctionsRebalanceThroughputTest(EventingRebalance):

    def __init__(self, *args):
        super().__init__(*args)
        self.on_update_success = 0

    @with_stats
    def execute_handler(self):
        self.pre_rebalance()

        on_update_success = self.get_on_update_success()
        time_taken = self.rebalance_time()
        self.on_update_success = \
            self.get_on_update_success() - on_update_success

        self.post_rebalance()
        time_taken = round(time_taken, 2)
        logger.info("Time taken for rebalance: {}sec".format(time_taken))
        return time_taken

    def run(self):
        self.set_functions()
        self.load()
        self.access_bg()

        time_taken = self.execute_handler()

        self.report_kpi(time_taken)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=self.on_update_success)
        )


class FunctionsRebalanceTimeThroughputTest(FunctionsRebalanceThroughputTest):
    def _report_kpi(self, time_elapsed):
        super()._report_kpi(time_elapsed)
        self.reporter.post(
            *self.metrics.eventing_rebalance_time(time=time_elapsed)
        )


class FunctionsRebalancePillowfightThroughputTest(FunctionsRebalanceThroughputTest):

    ALL_BUCKETS = True

    def load(self, *args):
        self.download_certificate()
        PerfTest.load(self, task=pillowfight_data_load_task)

    def access_bg(self, *args):
        self.download_certificate()

        PerfTest.access_bg(self, task=pillowfight_task)


class FunctionsRebalanceTimePillowfightThroughputTest(FunctionsRebalanceTimeThroughputTest):

    ALL_BUCKETS = True

    def load(self, *args):
        PerfTest.load(self, task=pillowfight_data_load_task)

    def access_bg(self, *args):
        self.download_certificate()

        PerfTest.access_bg(self, task=pillowfight_task)


class FunctionsScalingThroughputTest(EventingTest):

    def __init__(self, *args):
        super().__init__(*args)
        self.on_update_success = 0

    @with_stats
    def execute_handler(self):
        on_update_success = self.get_on_update_success()
        self.sleep()
        self.on_update_success = \
            self.get_on_update_success() - on_update_success

    def run(self):
        self.load()
        self.set_functions()

        self.execute_handler()

        self.report_kpi(self.test_config.access_settings.time)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=self.on_update_success)
        )


class TimerTest(EventingTest):

    @with_stats
    def process_timer_events(self):
        for name, filename in self.functions.items():
            self.monitor.wait_for_timer_event(node=self.eventing_nodes[0],
                                              function=name)
            break
        self.sleep()

    def run(self):
        self.set_functions()

        self.load()

        self.process_timer_events()

        self.report_kpi(self.time)


class TimerThroughputTest(TimerTest):

    def _report_kpi(self, time_elapsed):
        timer_msg_counter = self.get_timer_msg_counter()
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=timer_msg_counter)
        )


class TimerUndeployTest(TimerTest):

    def _report_kpi(self, time_to_undeploy):
        self.reporter.post(
            *self.metrics.function_time(time=time_to_undeploy,
                                        time_type="undeploy",
                                        initials="Time to undeploy(min)")
        )

    def wait_for_all_timers_creation(self):
        for name, filename in self.functions.items():
            self.monitor.wait_for_all_timer_creation(node=self.eventing_nodes[0],
                                                     function=name)

    @with_stats
    def load_and_wait_for_timers(self):
        self.load()
        self.wait_for_all_timers_creation()
        time_to_undeploy = self.undeploy()
        return time_to_undeploy

    def run(self):
        self.set_functions()
        time_to_undeploy = self.load_and_wait_for_timers()
        self.report_kpi(time_to_undeploy)


class TimerRebalanceThroughputTest(EventingRebalance):

    def __init__(self, *args):
        super().__init__(*args)
        self.timer_msg_counter = 0
        self.function_names = []
        for name, filename in self.functions.items():
            self.function_names.append(name)

    @with_stats
    def execute_handler(self):
        self.pre_rebalance()

        timer_msg_counter = self.get_timer_msg_counter()
        time_taken = self.rebalance_time()
        self.timer_msg_counter = \
            self.get_timer_msg_counter() - timer_msg_counter

        self.post_rebalance()
        time_taken = round(time_taken, 2)
        logger.info("Time taken for rebalance: {}sec".format(time_taken))
        return time_taken

    def wait_for_timer_event(self):
        for name, filename in self.functions.items():
            self.monitor.wait_for_timer_event(node=self.eventing_nodes[0],
                                              function=name)
            break

    def run(self):
        self.set_functions()
        self.load()
        self.wait_for_timer_event()

        time_taken = self.execute_handler()

        self.report_kpi(time_taken)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=self.timer_msg_counter)
        )


class TimerRebalanceTimeThroughputTest(TimerRebalanceThroughputTest):
    def _report_kpi(self, time_elapsed):
        super()._report_kpi(time_elapsed)
        self.reporter.post(
            *self.metrics.eventing_rebalance_time(time=time_elapsed)
        )


class FunctionsLatencyTest(EventingTest):
    def _report_kpi(self, time_elapsed):
        latency_stats = self.process_latency_stats()
        self.reporter.post(
            *self.metrics.function_latency(percentile=80.0, latency_stats=latency_stats)
        )


class FunctionsLatencyTestDestBucket(FunctionsLatencyTest):

    def load_dest_bucket(self):
        target_iterator = EventingDestBktTargetIterator(self.cluster_spec,
                                                        self.test_config,
                                                        self.key_prefix)

        load_settings = copy.deepcopy(self.test_config.load_settings)
        load_settings.doc_gen = self.dest_bkt_doc_gen
        self.load(settings=load_settings, target_iterator=target_iterator)

    def run(self):
        self.set_functions()

        self.load_dest_bucket()

        time_elapsed = self.load_access_and_wait()

        self.report_kpi(time_elapsed)
