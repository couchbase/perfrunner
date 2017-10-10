from cbagent.collectors import Collector


class EventingStats(Collector):

    COLLECTOR = "eventing_stats"

    def __init__(self, settings, test):
        super().__init__(settings)
        self.eventing_node = test.function_nodes[0]
        self.functions = test.functions

    def _get_processing_stats(self, function_name="perf-test1"):
        port = '25000'
        uri = "/getEventProcessingStats?name={}".format(function_name)
        samples = self.get_http(path=uri, server=self.eventing_node, port=port)
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
