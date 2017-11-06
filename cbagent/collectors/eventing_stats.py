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
        self.functions = test.functions

    def _get_dcp_events_remaining_stats(self, function_name):
        uri = "/getDcpEventsRemaining?name={}".format(function_name)
        stats = {}
        for node in self.eventing_nodes:
            remaining_events = self.get_http(path=uri, server=node,
                                             port=self.EVENTING_PORT, json=False)
            remaining_events = int(remaining_events)
            stats[node] = {"DcpEventsRemaining": remaining_events}
        return stats

    def sample(self):
        for name, function in self.functions.items():
            server_stats = self._get_dcp_events_remaining_stats(function_name=name)
            if server_stats:
                for server, stats in server_stats.items():
                    self.update_metric_metadata(server_stats.keys(), server=server)
                    self.store.append(stats, cluster=self.cluster,
                                      server=server,
                                      collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for node in self.eventing_nodes:
            self.mc.add_server(node)
