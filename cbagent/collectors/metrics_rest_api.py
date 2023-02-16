from cbagent.collectors.collector import Collector
from cbagent.settings import CbAgentSettings


class MetricsRestApiBase(Collector):

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        self.server_processes = settings.server_processes
        self.stats_uri = '/pools/default/stats/range/'
        self.stats_data = []

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes:
            self.mc.add_server(node)

    def add_stats(self, stats: dict, node: str = '', bucket: str = ''):
        if stats:
            self.update_metric_metadata(stats.keys(), server=node, bucket=bucket)
            self.store.append(stats, cluster=self.cluster, server=node, bucket=bucket,
                              collector=self.COLLECTOR)

    def get_stats(self):
        raise NotImplementedError()

    def sample(self):
        raise NotImplementedError()


class MetricsRestApiProcesses(MetricsRestApiBase):

    COLLECTOR = "metrics_rest_api_processes"

    PROCESSES = (
        "cbas",
        "cbft",
        "cbq-engine",
        "eventing-produc",
        "goxdcr",
        "indexer",
        "java",
        "memcached",
        "ns_server",
        "projector",
        "prometheus",
    )

    METRICS = (
        "sysproc_cpu_utilization",
        "sysproc_mem_resident"
    )

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        self.stats_data = [
            {
                "metric": [
                    {"label": "name", "value": metric},
                    {"label": "proc", "value": proc}
                ],
                "step": 1,
                "start": -1
            }
            for metric in self.METRICS
            for proc in set(self.server_processes) & set(self.PROCESSES)
        ]

    def get_stats(self) -> dict:
        samples = self.post_http(path=self.stats_uri, json_data=self.stats_data)
        stats = {}
        for data in samples:
            for metric in data['data']:
                node = metric['metric']['nodes'][0].split(':')[0]
                metric_name = metric['metric']['name']
                proc = metric['metric']['proc']
                value = float(metric['values'][-1][-1])
                title = '{}_{}'.format(proc, metric_name)
                if node not in stats:
                    stats[node] = {title: value}
                else:
                    stats[node][title] = value
        return stats

    def sample(self):
        for node, stats in self.get_stats().items():
            self.add_stats(stats, node=node)


class MetricsRestApiMetering(MetricsRestApiBase):

    COLLECTOR = "metrics_rest_api_metering"

    METRICS = (
        "meter_ru_total",
        "meter_wu_total",
        "meter_cu_total"
    )

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        self.stats_data = [
            {
                "metric": [
                    {"label": "name", "value": metric}
                ],
                "applyFunctions": ["irate"],
                "step": 1,
                "start": -1
            }
            for metric in self.METRICS
        ]

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)

    def get_stats(self) -> dict:
        samples = self.post_http(path=self.stats_uri, json_data=self.stats_data)
        stats = {}
        for data in samples:
            for metric in data['data']:
                if 'bucket' in metric['metric']:
                    metric_name = metric['metric']['name']
                    bucket = metric['metric']['bucket']
                    instance = metric['metric']['instance']
                    value = float(metric['values'][-1][-1])
                    title = '{}_{}'.format(instance, metric_name)
                    if bucket not in stats:
                        stats[bucket] = {title: value}
                    elif title not in stats[bucket]:
                        stats[bucket][title] = value
                    else:
                        stats[bucket][title] += value
        return stats

    def sample(self):
        for bucket, stats in self.get_stats().items():
            bucket = self.serverless_db_names.get(bucket, bucket)
            self.add_stats(stats, bucket=bucket)


class MetricsRestApiDeduplication(MetricsRestApiBase):

    COLLECTOR = "metrics_rest_api_dedup"

    METRICS = {
        "kv_ep_total_deduplicated": "kv_ep_total_deduplicated_rate",
        "kv_ep_total_deduplicated_flusher": "kv_ep_total_deduplicated_flusher_rate",  # >=7.2.0-5172
        "kv_ep_total_enqueued": "kv_ep_total_enqueued_rate"
    }

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        self.stats_data = [
            {
                "metric": [
                    {"label": "name", "value": metric}
                ],
                "applyFunctions": ["irate"],
                "step": 1,
                "start": -1
            }
            for metric in self.METRICS
        ]

    def update_metadata(self):
        self.mc.add_cluster()

        for node in self.nodes:
            self.mc.add_server(node)

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)

    def get_stats(self) -> dict:
        samples = self.post_http(path=self.stats_uri, json_data=self.stats_data)
        metrics = {}
        for data in samples:
            for metric in data['data']:
                if 'bucket' in metric['metric']:
                    metric_name = self.METRICS[metric['metric']['name']]
                    node = metric['metric']['nodes'][0].split(':')[0]
                    bucket = metric['metric']['bucket']
                    value = float(metric['values'][-1][-1])
                    if node not in metrics:
                        metrics[node] = {bucket: {metric_name: value}}
                    elif bucket not in metrics[node]:
                        metrics[node][bucket] = {metric_name: value}
                    elif metric_name not in metrics[node][bucket]:
                        metrics[node][bucket][metric_name] = value

        return metrics

    def sample(self):
        current_stats = self.get_stats()
        for node, per_bucket_stats in current_stats.items():
            for bucket, stats in per_bucket_stats.items():
                bucket = self.serverless_db_names.get(bucket, bucket)
                self.add_stats(stats, node=node, bucket=bucket)
