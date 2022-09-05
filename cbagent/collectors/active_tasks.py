from cbagent.collectors.collector import Collector


class ActiveTasks(Collector):

    COLLECTOR = "active_tasks"

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)

    def _get_tasks(self):
        bucket_compaction_tasks = {}

        for task in self.get_http(path="/pools/default/tasks"):
            if task["type"] == "bucket_compaction":
                bucket_compaction_tasks[task["bucket"]] = task["progress"]
            elif task["type"] == "rebalance":
                yield "rebalance_progress", task.get("progress", 0), None

        for bucket in self.get_buckets():
            progress = bucket_compaction_tasks.get(bucket, 0)
            yield "bucket_compaction_progress", progress, bucket

    def sample(self):
        for task, progress, bucket in self._get_tasks():
            self.update_metric_metadata(metrics=(task, ), bucket=bucket)
            self.append_to_store(data={task: progress},
                                 cluster=self.cluster, bucket=bucket,
                                 collector=self.COLLECTOR)
