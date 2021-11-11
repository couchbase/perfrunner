from cbagent.collectors import SecondaryStats


class SecondaryStorageStatsMM(SecondaryStats):

    COLLECTOR = "secondary_storage_stats_mm"

    METRICS = "Allocated", "resident", "metadata"

    def __init__(self, settings):
        super().__init__(settings)
        self.index_node = settings.index_node

    def _get_secondary_storage_stats_mm(self):
        server = self.index_node
        port = '9102'
        uri = "/stats/storage/mm"
        samples = self.get_http(path=uri, server=server, port=port, json=False)

        stats = {}
        for line in samples.split("\n"):
            if "resident" in line and "Allocated" in line:
                for s in line.split(","):
                    v = s.split(":")
                    if v[0].strip() in self.METRICS:
                        key = "mm_" + v[0].strip().lower()
                        stats[key] = int(v[1].split("(")[0])
        return stats

    def sample(self):
        stats = self._get_secondary_storage_stats_mm()
        if stats:
            self.update_metric_metadata(stats.keys())
            self.store.append(stats, cluster=self.cluster,
                              collector=self.COLLECTOR)
