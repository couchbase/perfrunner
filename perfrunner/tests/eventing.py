import json

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict
from perfrunner.tests import PerfTest, TargetIterator


class EventingTest(PerfTest):

    """Eventing test base class.

    This is base class for eventing related operations required
    to measure eventing performance parameters.
    """

    FUNCTION_SAMPLE_FILE = "tests/eventing/config/function_sample.json"
    FUNCTION_ENABLE_SAMPLE_FILE = "tests/eventing/config/enable_function_sample.json"
    COLLECTORS = {'eventing_stats': True, 'ns_server_system': True}

    def __init__(self, *args):
        super().__init__(*args)

        self.functions = self.test_config.eventing_settings.functions
        self.worker_count = self.test_config.eventing_settings.worker_count
        self.cpp_worker_thread_count = self.test_config.eventing_settings.cpp_worker_thread_count
        self.timer_worker_pool_size = self.test_config.eventing_settings.timer_worker_pool_size
        self.memory_quota = self.test_config.eventing_settings.memory_quota
        self.worker_queue_cap = self.test_config.eventing_settings.worker_queue_cap
        self.time = self.test_config.access_settings.time

        self.eventing_nodes = self.cluster_spec.servers_by_role('eventing')

        for master in self.cluster_spec.masters:
            self.rest.add_rbac_user(
                host=master,
                bucket="eventing",
                password="password",
                roles=['admin'],
            )

        self.target_iterator = TargetIterator(self.cluster_spec, self.test_config, "eventing")

    def set_functions(self):
        with open(self.FUNCTION_SAMPLE_FILE) as f:
            func = json.load(f)

        func["settings"]["worker_count"] = self.worker_count
        func["settings"]["cpp_worker_thread_count"] = self.cpp_worker_thread_count
        func["settings"]["timer_worker_pool_size"] = self.timer_worker_pool_size
        func["settings"]["memory_quota"] = self.memory_quota
        func["settings"]["worker_queue_cap"] = self.worker_queue_cap
        for name, filename in self.functions.items():
            with open(filename, 'r') as myfile:
                code = myfile.read()
                func["appname"] = name
                func["appcode"] = code
            function = json.dumps(func)
            self.rest.create_function(node=self.eventing_nodes[0], payload=function, name=name)
            self.rest.deploy_function(node=self.eventing_nodes[0], payload=function, name=name)
            self.monitor.wait_for_bootstrap(nodes=self.eventing_nodes, function=name)

    def process_latency_stats(self):
        latency_stats = {}
        for name, file in self.functions.items():
            stats = self.monitor.wait_for_latency_stats(
                node=self.eventing_nodes[0], name=name)
            logger.info("Latency stats for function {function}:{stats}".
                        format(function=name, stats=pretty_dict(stats)))
            stats = sorted(stats.items(), key=lambda x: int(x[0]))
            latency_stats[name] = stats
        return latency_stats

    def get_success_stats(self):
        on_update_success = 0
        for name, file in self.functions.items():
            for node in self.eventing_nodes:
                stats = self.monitor.wait_for_execution_stats(node=node, name=name)
                logger.info("Execution stats for {node} and {function}: {stats}"
                            .format(node=node, function=name, stats=pretty_dict(stats)))
                on_update_success += stats["on_update_success"]
        return on_update_success

    @timeit
    @with_stats
    def load_access_and_wait(self):
        self.load()
        self.access_bg()
        self.sleep()

    def validate_failures(self):
        for name, file in self.functions.items():
            for node in self.eventing_nodes:
                stats = self.monitor.wait_for_execution_stats(node=node, name=name)
                logger.info("Execution stats for {node} and {function}: {stats}"
                            .format(node=node, function=name, stats=pretty_dict(stats)))
                for stat, value in stats.items():
                    if "success" not in stat and value != 0:
                        raise Exception(
                            '{function}: {node}: {stat} is not zero'.format(
                                function=name, node=node, stat=stat))

    def run(self):
        self.set_functions()

        time_elapsed = self.load_access_and_wait()

        self.report_kpi(time_elapsed)

        self.validate_failures()


class FunctionsThroughputTest(EventingTest):
    def _report_kpi(self, time_elapsed):
        events_successfully_processed = self.get_success_stats()
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name=None,
                                              events_processed=events_successfully_processed)
        )


class FunctionsScalingThroughputTest(EventingTest):

    def __init__(self, *args):
        super().__init__(*args)
        self.on_update_success = 0

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
        self.validate_failures()

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

        self.validate_failures()


class TimerThroughputTest(TimerTest):

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.function_throughput(time=time_elapsed,
                                              event_name="DOC_TIMER_EVENTS",
                                              events_processed=0)
        )


class FunctionsLatencyTest(EventingTest):
    def _report_kpi(self, time_elapsed):
        latency_stats = self.process_latency_stats()
        self.reporter.post(
            *self.metrics.function_latency(percentile=80.0, latency_stats=latency_stats)
        )
