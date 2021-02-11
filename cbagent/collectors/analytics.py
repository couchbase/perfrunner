from typing import List

from cbagent.collectors.collector import Collector


class AnalyticsStats(Collector):

    COLLECTOR = "analytics"

    PORT = 9110

    def __init__(self, settings, servers: List[str]):
        super().__init__(settings)

        self.servers = servers

    def update_metadata(self):
        self.mc.add_cluster()

        for server in self.servers:
            self.mc.add_server(server)

    def get_stats(self, server: str) -> dict:
        return self.get_http(path='/analytics/node/stats',
                             server=server,
                             port=self.PORT)

    def sample(self):
        for server in self.servers:
            stats = self.get_stats(server)
            self.update_metric_metadata(stats.keys(), server=server)
            self.store.append(stats,
                              cluster=self.cluster,
                              server=server,
                              collector=self.COLLECTOR)
