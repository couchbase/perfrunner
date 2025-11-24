from cbagent.collectors.collector import Collector
from cbagent.settings import CbAgentSettings


class MetricsRestApiBase(Collector):

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        self.server_processes = settings.server_processes
        self.stats_uri = '/pools/default/stats/range/'
        self.stats_data = []
        self._hostname_map_cache = {}

    def _internal_to_external_hostnames(self):
        """Generate a mapping of internal hostnames to external hostnames.

        The internal hostname is used within the CB cluster and is what is returned by the metrics
        REST API.

        In cbmonitor reports though we use the external hostname, so for consistency with all the
        other metrics we need to map the internal hostnames to the external ones.
        """
        # Get the cluster info again as the topology may have changed
        cluster_info = self.get_http(path="/pools/default")
        self._hostname_map_cache = {}
        for node_info in cluster_info["nodes"]:
            internal_hostname = node_info["hostname"].split(":")[0]
            otp_name = node_info["otpNode"].split("@")[-1]
            self._hostname_map_cache[internal_hostname] = otp_name

    def get_external_hostnames(self, node: str) -> str:
        """Get the external hostnames for a node from the cached data.

        If the value is not found in the cache, we invalidate the cache and try again.
        If the value is still not found, we return the node itself.
        """
        try:
            return self._hostname_map_cache[node]
        except KeyError:
            # Invalidate the cache and try again
            self._internal_to_external_hostnames()
            # Return the node itself if the value is not found after reloading the cache
            return self._hostname_map_cache.get(node, node)

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
            self.add_stats(stats, node=self.get_external_hostnames(node))


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
                self.add_stats(stats, node=self.get_external_hostnames(node), bucket=bucket)


class MetricsRestApiThroughputCollection(MetricsRestApiBase):

    COLLECTOR = "metrics_rest_api_collection_throughput"

    METRIC = "kv_collection_ops"

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)

        self.target_groups_per_bucket = {
            bucket: {
                options.get('stat_group')
                for _, collections in scopes.items()
                for _, options in collections.items()
            } - {None}
            for bucket, scopes in self.collections.items()
        }

        self.minimal_targets = self.calculate_minimal_targets()

        self.stats_data = []
        for target in self.minimal_targets:
            metric = [
                {"label": label, "value": value}
                for label, value in zip(['bucket', 'scope', 'collection'], target.split(':'))
            ]
            self.stats_data.append({
                "metric": [{"label": "name", "value": self.METRIC}] + metric,
                "applyFunctions": ["irate", "sum"],
                "nodesAggregation": "sum",
                "step": 1,
                "start": -1
            })

    def get_num_collections(self, target_str: str) -> int:
        """Given a "target" string, return the number of collections that the target refers to.

        The "target" string is of the form <bucket>[.<scope>[.<collection>]]. E.g.:

            - "bucket-1.scope-1.collection-1" -> returns 1
            - "bucket-1.scope-1"              -> returns number of colls in scope-1
            - "bucket-1"                      -> returns number of colls in bucket-1
        """
        target = dict(zip(['bucket', 'scope', 'collection'], target_str.split(':')))

        if not target or target.get('collection'):
            return 1
        elif target and (scope := target.get('scope')):
            return len(self.collections[target['bucket']][scope])

        return sum(len(colls) for colls in self.collections[target['bucket']].values())

    @staticmethod
    def bucket_stat_group(bucket: str, group: str) -> str:
        if group != '':
            return '{}_{}'.format(bucket, group)
        return bucket

    def calculate_minimal_targets(self) -> dict[str, str]:
        """Construct minimal set of "targets" which cover all stat groups.

        The collection config specifies stat groups at a collection-level granularity (highest
        granularity). The CB server Metrics REST API allows us to get stats at a lower granularity,
        (scope- or bucket-level) which we may be able to exploit if the stat groups allow for it.

        For example, if all the collections in a scope belong to the same stat group, then we can
        just query the REST API for the scope-level stats for that scope.

        This method constructs the shortest dictionary of <target>: <stat group> pairs which
        completely covers all the stat groups.
        """
        groups = {}
        for bucket, scopes in self.collections.items():
            scope_groups = set()
            for scope, collections in scopes.items():
                coll_groups = set()
                for collection, options in collections.items():
                    target = '{}:{}:{}'.format(bucket, scope, collection)

                    if g := options.get('stat_group'):
                        coll_groups.add(g)
                        scope_groups.add(g)

                        if g not in groups:
                            groups[g] = [target]
                        else:
                            groups[g].append(target)

                if len(coll_groups) == 1:
                    g = coll_groups.pop()
                    for _ in collections:
                        groups[g].pop(-1)
                    groups[g].append('{}:{}'.format(bucket, scope))

            if len(scope_groups) == 1:
                g = scope_groups.pop()
                for _ in scopes:
                    groups[g].pop(-1)
                groups[g].append(bucket)

        groups_inversed = {target: group for group, targets in groups.items() for target in targets}
        return groups_inversed

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            for group in self.target_groups_per_bucket[bucket]:
                bucket_group = self.bucket_stat_group(bucket, group)
                self.mc.add_bucket(bucket_group)

    def get_stats(self) -> dict:
        samples = self.post_http(path=self.stats_uri, json_data=self.stats_data)
        metrics = {}
        for data, (target, stat_group) in zip(samples, self.minimal_targets.items()):
            bucket = target.split(':')[0]
            value = float(data['data'][0]['values'][-1][1]) / self.get_num_collections(target)
            if stat_group not in metrics:
                metrics[stat_group] = {bucket: {self.METRIC: value}}
            elif bucket not in metrics[stat_group]:
                metrics[stat_group][bucket] = {self.METRIC: value}
            else:
                metrics[stat_group][bucket][self.METRIC] += value
        return metrics

    def sample(self):
        current_stats = self.get_stats()
        for stat_group, per_bucket_stats in current_stats.items():
            for bucket, stats in per_bucket_stats.items():
                bucket_group = self.bucket_stat_group(bucket, stat_group)
                self.add_stats(stats, bucket=bucket_group)


class MetricsRestApiAppTelemetry(MetricsRestApiBase):
    COLLECTOR = "metrics_rest_api_app_telemetry"

    METRICS = (
        "cm_app_telemetry_curr_connections",
        "kv_current_external_client_connections",
        "sdk_.*(_r_total|_r_cancelled|_r_timedout)",
        # This covers the per service meters as described in RFC-0084
    )

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        self.stats_data = [
            {
                "metric": [{"label": "name", "value": metric, "operator": "=~"}],
                "step": 1,
                "start": -1,
            }
            for metric in self.METRICS
        ]

    def get_stats(self) -> dict:
        samples = self.post_http(path=self.stats_uri, json_data=self.stats_data)
        metrics = {}
        for data in samples:
            for metric in data["data"]:
                node = metric["metric"]["nodes"][0].split(":")[0]
                metric_name = metric["metric"]["name"]
                value = float(metric["values"][-1][-1])
                if metric_name == "kv_current_external_client_connections":
                    sdk_name = metric["metric"]["sdk"].split("/")[0]
                    metric_name = f"{metric_name}_{sdk_name}"
                if node not in metrics:
                    metrics[node] = {metric_name: value}
                else:
                    metrics[node][metric_name] = value
        return metrics

    def sample(self):
        for node, stats in self.get_stats().items():
            self.add_stats(stats, node=self.get_external_hostnames(node))

class MetricsRestApiDeks(MetricsRestApiBase):

    COLLECTOR = "metrics_rest_api_deks"

    METRICS = [
        "cm_key_manager_deks_in_use",
        "cm_key_manager_drop_deks_total",
        "cm_key_manager_generate_key_total",
        "cm_key_manager_retire_key_total"
    ]

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        self.stats_data = [
            {
                "metric": [
                    {"label": "name", "value": metric}
                ],
                "step": 1,
                "start": -1
            }
            for metric in self.METRICS
        ]

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes:
            self.mc.add_server(node)

    def get_stats(self) -> dict:
        samples = self.post_http(path=self.stats_uri, json_data=self.stats_data)
        metrics = {}
        for data in samples:
            for metric in data['data']:
                node = metric['metric']['nodes'][0].split(':')[0]
                metric_name = metric['metric']['name']
                value = float(metric['values'][-1][-1])
                if node not in metrics:
                    metrics[node] = {metric_name: value}
                else:
                    metrics[node][metric_name] = value
        return metrics

    def sample(self):
        current_stats = self.get_stats()
        for node, stats in current_stats.items():
            self.add_stats(stats, node=self.internal_to_external_hostnames[node])
