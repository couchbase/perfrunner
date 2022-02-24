from fabric.api import run, settings

from cbagent.collectors.collector import Collector


class EventingStats(Collector):

    COLLECTOR = "eventing_stats"
    EVENTING_PORT = 8096

    def __init__(self, settings, test):
        super().__init__(settings)
        self.eventing_node = test.eventing_nodes[0]
        self.functions = test.functions
        self.eventing_nodes = test.eventing_nodes

    def get_eventing_stats(self, server, full_stats=False):
        api = '/api/v1/stats'
        if full_stats:
            api += "?type=full"
        return self.get_http(server=server, port=self.EVENTING_PORT, path=api)

    def _get_processing_stats(self):
        samples = {}
        for node in self.eventing_nodes:
            stats = self.get_eventing_stats(server=node)
            for fun_stat in stats:
                if "function_name" in fun_stat:
                    samples[fun_stat["function_name"]] = fun_stat["event_processing_stats"]
        return samples

    def sample(self):
        all_stats = self._get_processing_stats()
        for name, function in self.functions.items():
            stats = all_stats[name]
            if stats:
                self.update_metric_metadata(stats.keys(), bucket=name)
                self.store.append(stats, cluster=self.cluster,
                                  bucket=name, collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for name, function in self.functions.items():
            self.mc.add_bucket(name)


class EventingPerNodeStats(EventingStats):

    COLLECTOR = "eventing_per_node_stats"

    def __init__(self, settings, test):
        super().__init__(settings, test)

    def _get_dcp_events_remaining_stats(self):
        events_remaining_stats = {}
        for node in self.eventing_nodes:
            stats = self.get_eventing_stats(server=node)
            events_remaining = 0
            for fun_stat in stats:
                if "events_remaining" in fun_stat:
                    events_remaining += fun_stat["events_remaining"]["dcp_backlog"]
                    events_remaining_stats[node] = {"DcpEventsRemaining": events_remaining}
        return events_remaining_stats

    def sample(self):
        server_stats = self._get_dcp_events_remaining_stats()
        if server_stats:
            for server, stats in server_stats.items():
                self.update_metric_metadata(stats.keys(), server=server)
                self.store.append(stats, cluster=self.cluster,
                                  server=server,
                                  collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for node in self.eventing_nodes:
            self.mc.add_server(node)


class EventingPerHandlerStats(EventingStats):

    COLLECTOR = "eventing_per_handler_stats"

    def __init__(self, settings, test):
        super().__init__(settings, test)

    def _get_handler_stats(self, function_name):
        handler_stats = dict()
        handler_stats[function_name] = dict()
        on_update_success = 0
        for node in self.eventing_nodes:
            stats = self.get_eventing_stats(server=node)
            for stat in stats:
                if "function_name" in function_name and stat["function_name"] == function_name:
                    if "execution_stats" in stat:
                        on_update_success += stat["execution_stats"]["on_update_success"]
        handler_stats[function_name]["on_update_success"] = on_update_success
        return handler_stats

    def sample(self):
        for name, function in self.functions.items():
            handler_stats = self._get_handler_stats(function_name=name)
            if handler_stats:
                stats = handler_stats[name]
                self.update_metric_metadata(stats.keys(),
                                            bucket=name)
                self.store.append(stats, cluster=self.cluster,
                                  bucket=name,
                                  collector=self.COLLECTOR)


class EventingConsumerStats(EventingPerNodeStats):

    COLLECTOR = "eventing_consumer_stats"

    PS_CMD = "ps -eo pid,rss,vsize,comm " \
             "| grep '{}' " \
             "| awk 'BEGIN {{FS = \" \"}} ; {{sum+=$2}} END {{print sum}}'"

    TOP_CMD = "top -b n1 -d1 -p{} " \
              "| grep '{}' " \
              "| awk 'BEGIN {{FS = \" \"}} ; {{sum+=$9}} END {{print sum}}'"

    def __init__(self, settings, test):
        super().__init__(settings, test)

    def _get_consumer_pids(self, function_name):
        node_pids = {}
        for node in self.eventing_nodes:
            stats = self.get_eventing_stats(server=node)
            worker_pids = {}
            for fun_stat in stats:
                if "function_name" in function_name and fun_stat["function_name"] == function_name:
                    worker_pids = fun_stat["worker_pids"]
                    break
            node_pids[node] = worker_pids
        return node_pids

    def _get_pid_stats(self, node_pids):
        stats = {}
        for node, pids in node_pids.items():
            stats[node] = {}
            if pids:
                grep_text_ps = ""
                pid_list_top = []
                pid_list_top_str = ""
                grep_text_top = []
                grep_text_top_str = "^\\s*"
                counter = 0
                # run top command in batches of 20 pids as per top command limitation
                for pid in pids.values():
                    grep_text_ps += '^[[:space:]]*{}\\|'.format(pid)
                    pid_list_top_str += '{},'.format(pid)
                    grep_text_top_str += '{}\\|'.format(pid)
                    counter += 1
                    if counter == 20:
                        counter = 0
                        pid_list_top.append(pid_list_top_str[:-1])
                        grep_text_top.append(grep_text_top_str[:-2])
                        pid_list_top_str = ""
                        grep_text_top_str = "^\\s*"
                grep_text_ps = grep_text_ps[:-2]
                with settings(host_string=node):
                    rss_used = run(self.PS_CMD.format(grep_text_ps))
                    cpu_used = 0
                    for pid_list, grep_text in zip(pid_list_top, grep_text_top):
                        cpu_used += float(run(self.TOP_CMD.format(pid_list, grep_text)))
                # convert rss to bytes as ps commands gives rss in KB
                stats[node]["eventing_consumer_rss"] = float(rss_used) * 1024
                stats[node]["eventing_consumer_cpu"] = cpu_used
        return stats

    def sample(self):
        for name, function in self.functions.items():
            node_pids = self._get_consumer_pids(function_name=name)
            server_stats = self._get_pid_stats(node_pids)
            if server_stats:
                for server, stats in server_stats.items():
                    self.update_metric_metadata(stats.keys(), server=server,
                                                bucket=name)
                    self.store.append(stats, cluster=self.cluster,
                                      server=server, bucket=name,
                                      collector=self.COLLECTOR)
