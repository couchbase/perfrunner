import re

from logger import logger

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class DCPTest(PerfTest):
    """
    The test is for DCP performance.
    """

    COLLECTORS = {}

    def __init__(self, *args):
        super(DCPTest, self).__init__(*args)
        self.num_connections = self.test_config.dcp_settings.num_connections
        self.bucket = self.test_config.dcp_settings.bucket
        self.username, self.password = self.cluster_spec.rest_credentials
        self.items = self.test_config.load_settings.items


class DCPThroughputTest(DCPTest):

    """
    The test measures time to get number of dcp messages and calculates throughput.
    """

    OUTPUT_FILE = "dcpstatsfile"

    def _report_kpi(self, throughput):
        self.reporter.post_to_sf(
            *self.metric_helper.get_dcp_meta(value=throughput)
        )

    @with_stats
    def run_dcptest_script(self):
        local.run_dcptest_script(self)

    def get_throughput(self):
        # get throughput from OUTPUT_FILE for posting to showfast
        with open(self.OUTPUT_FILE) as file:
            output_text = file.read()
            groups = re.search(
                r"Throughput = [^\d]*(\d*).*?",
                output_text)
            throughput = int(groups.group(1))
        return throughput

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.run_dcptest_script()

        throughput = self.get_throughput()

        logger.info("Throughput = {}".format(throughput))
        self.report_kpi(throughput)
