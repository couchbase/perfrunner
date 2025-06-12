import json
from datetime import datetime

from logger import logger
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import ShowFastReporter
from perfrunner.tests import PerfTest


class FIOTest(PerfTest):

    def __init__(self, cluster_spec, test_config, verbose):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = RemoteHelper(cluster_spec, verbose)
        build = datetime.now().strftime("%Y-%m-%d")
        self.reporter = ShowFastReporter(cluster_spec, test_config, build)
        self.dynamic_infra = False
        self.metrics = MetricHelper(self)

    def __exit__(self, *args, **kwargs):
        pass

    @staticmethod
    def _parse(results: dict) -> dict:
        """Parse the fio json output from each node."""
        stats = {}
        for host, output in results.items():
            host_output = json.loads(output)
            json_output = host_output.get("jobs", [])
            stats[host] = {}
            # Get read and write iops from each job
            for job in json_output:
                job_name = job.get("jobname", "")
                job_iops = stats[host].get(job_name, 0)
                stats[host][job_name] = (
                    job_iops
                    + job.get("read", {}).get("iops", 0)
                    + job.get("write", {}).get("iops", 0)
                    + job.get("trim", {}).get("iops", 0)
                )
            # replace the output with the deserialised output so we store it as a json file
            results[host] = host_output

        return stats

    def _report_kpi(self, stats: dict):
        logger.info(f"Fio stats: {stats}")
        for host, data in stats.items():
            for job_name, iops in data.items():
                self.reporter.post(
                    *self.metrics.fio_iops(iops, self.cluster_spec.name, host, job_name)
                )

    def run(self):
        config = self.test_config.fio["config"]
        logger.info(f"Running fio job: {config}")
        results = self.remote.fio(config)
        self.report_kpi(self._parse(results))
        # save the results to a file
        with open("fio.json", "w") as file:
            json.dump(results, file, indent=4)
