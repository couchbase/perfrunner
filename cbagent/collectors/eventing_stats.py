from fabric.api import run, settings

from cbagent.collectors import Collector


class EventingStats(Collector):

    COLLECTOR = "eventing_stats"
    EVENTING_PORT = 8096

    def __init__(self, settings, test):
        super().__init__(settings)
        self.eventing_node = test.eventing_nodes[0]
        self.functions = test.functions

    def _get_processing_stats(self, function_name):
        uri = "/getAggEventProcessingStats?name={}".format(function_name)
        samples = self.get_http(path=uri, server=self.eventing_node, port=self.EVENTING_PORT)
        return samples

    def sample(self):
        for name, function in self.functions.items():
            stats = self._get_processing_stats(function_name=name)
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
        self.eventing_nodes = test.eventing_nodes

    def _get_dcp_events_remaining_stats(self, function_name):
        uri = "/getDcpEventsRemaining?name={}".format(function_name)
        stats = {}
        for node in self.eventing_nodes:
            remaining_events = self.get_http(path=uri, server=node,
                                             port=self.EVENTING_PORT)
            remaining_events = remaining_events["dcp_backlog"]
            stats[node] = {"DcpEventsRemaining": remaining_events}
        return stats

    def sample(self):
        for name, function in self.functions.items():
            server_stats = self._get_dcp_events_remaining_stats(function_name=name)
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
        uri = "/getConsumerPids?name={}".format(function_name)
        node_pids = {}
        for node in self.eventing_nodes:
            pids = self.get_http(path=uri, server=node,
                                 port=self.EVENTING_PORT)
            node_pids[node] = pids
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
                grep_text_top_str = "^\s*"
                counter = 0
                # run top command in batches of 20 pids as per top command limitation
                for pid in pids.values():
                    grep_text_ps += '^[[:space:]]*{}\|'.format(pid)
                    pid_list_top_str += '{},'.format(pid)
                    grep_text_top_str += '{}\|'.format(pid)
                    counter += 1
                    if counter == 20:
                        counter = 0
                        pid_list_top.append(pid_list_top_str[:-1])
                        grep_text_top.append(grep_text_top_str[:-2])
                        pid_list_top_str = ""
                        grep_text_top_str = "^\s*"
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
