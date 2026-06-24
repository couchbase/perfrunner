from typing import Optional

from cbagent.collectors.collector import CouchbaseCollector
from cbagent.settings import CbAgentSettings
from perfrunner.tests import PerfTest


class WorkflowMetadataStats(CouchbaseCollector):
    COLLECTOR = "ai_workflow_stats"
    COLLECTOR_FLAG = "ai_workflow_stats"
    SKIP_ON_DYNAMIC = True
    METRICS = "ai_workflow_stats"

    def __init__(self, settings: CbAgentSettings, test: Optional[PerfTest] = None):
        super().__init__(settings)

    def sample(self):
        workflows = self.session.get_all_workflows(self.master_node)
        for workflow in workflows:
            name = workflow.get("data", {}).get("name")
            # Here we make an assumption that perfrunner will not retry/rerun the same workflow,
            # so the first and hopefully the only run metadata is the one we want
            run_metadata = workflow.get("data", {}).get("workflowRuns", [{}])[0]
            stats = {
                **run_metadata.get("udsMetadata", {}),
                **run_metadata.get("vectorizationMetadata", {}),
            }
            if stats:
                self.store.append(
                    stats, cluster=self.cluster, bucket=name, collector=self.COLLECTOR
                )

    def update_metadata(self):
        self.mc.add_cluster()
        self.mc.add_metric(self.METRICS, collector=self.COLLECTOR)
