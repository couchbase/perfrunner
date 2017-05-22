import re

from logger import logger

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class DCPThroughputTest(PerfTest):

    """
    The test measures time to get number of dcp messages and calculates throughput.
    """

    OUTPUT_FILE = "dcpstatsfile"

    def _report_kpi(self, throughput):
        self.reporter.post(
            value=throughput
        )

    @with_stats
    def access(self, *args):
        username, password = self.cluster_spec.rest_credentials

        for target in self.target_iterator:
            local.run_dcptest_script(
                host_port=target.node,
                username=username,
                password=password,
                bucket=target.bucket,
                num_items=self.test_config.load_settings.items,
                num_connections=self.test_config.dcp_settings.num_connections,
                output_file=self.OUTPUT_FILE,
            )

    def get_throughput(self):
        # get throughput from OUTPUT_FILE for posting to showfast
        with open(self.OUTPUT_FILE) as fh:
            output_text = fh.read()
            groups = re.search(
                r"Throughput = [^\d]*(\d*).*?",
                output_text)
            throughput = int(groups.group(1))
        return throughput

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.access()

        throughput = self.get_throughput()

        logger.info("Throughput = {}".format(throughput))
        self.report_kpi(throughput)
