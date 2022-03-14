import shutil
from pathlib import Path

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.local import extract_cb
from perfrunner.helpers.tableau import TableauRestHelper, TableauTerminalHelper
from perfrunner.tests.analytics import CH2Test


class AnalyticsConnectorTest(CH2Test):

    ANALYTICS_COLLECTIONS = ['customer', 'orders']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.extra_config_path = 'tests/connectors/analytics/config/'

    def create_analytics_collections(self):
        logger.info('Creating analytics collections.')
        for coll in self.ANALYTICS_COLLECTIONS:
            statement = 'ALTER COLLECTION bench.ch2.{} ENABLE ANALYTICS;'.format(coll)
            logger.info('Running: {}'.format(statement))
            self.rest.exec_analytics_statement(self.analytics_node, statement)

    def create_tabular_views(self):
        with open('{}/tabular_views.n1ql'.format(self.extra_config_path)) as f:
            statements = f.read()

        logger.info('Creating tabular views')
        for statement in statements.split('\n\n'):
            self.rest.exec_analytics_statement(self.analytics_node, statement)

    def sync(self):
        self.disconnect_link()
        self.create_analytics_collections()
        self.connect_link()
        for bucket in self.test_config.buckets:
            self.monitor.monitor_data_synced(self.data_node, bucket, self.analytics_node)

    def _exec_analytics_statement(self, statement: str, outfile: str):
        api = 'http://{}:8095/analytics/service'.format(self.analytics_node)
        data = {'statement': statement}

        logger.info('Executing analytics statement and saving result to file:\n\t{}'
                    .format(statement))

        with self.rest.post(url=api, data=data, stream=True) as r:
            with open(outfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

    def _report_kpi(self, time_elapsed: int, op: str):
        self.reporter.post(
            *self.metrics.custom_elapsed_time(time_elapsed, op)
        )

    def setup_run(self):
        extract_cb(filename='couchbase.rpm')
        self.restore_local()
        self.wait_for_persistence()
        self.restart()
        self.sync()
        self.create_tabular_views()

    def run(self):
        self.setup_run()


class BaseDataExtractionControlTest(AnalyticsConnectorTest):

    @with_stats
    @timeit
    def extract_collection(self):
        self._exec_analytics_statement(
            'select value c from bench.ch2.customer as c;',
            'extract_collection.out'
        )

    def run(self):
        self.setup_run()
        time_elapsed = self.extract_collection()
        self.report_kpi(time_elapsed, op='extract_collection')


class TabularViewExtractionControlTest(AnalyticsConnectorTest):

    @with_stats
    @timeit
    def extract_tabular_view(self):
        self._exec_analytics_statement(
            'set `sql_compat` "true"; select * from bench.ch2.customer_view;',
            'extract_tabular_view.out'
        )

    def run(self):
        self.setup_run()
        time_elapsed = self.extract_tabular_view()
        self.report_kpi(time_elapsed, op='extract_tabular_view')


class AdHocQueryCollectionsControlTest(AnalyticsConnectorTest):

    @with_stats
    @timeit
    def adhoc_query_collections(self):
        self._exec_analytics_statement(
            (
                'select c.c_state, sum(o.o_ol_cnt) '
                'from bench.ch2.orders as o, bench.ch2.customer as c '
                'where o.o_c_id = c.c_id '
                'group by c.c_state;'
            ),
            'ad-hoc_query_collections.out'
        )

    def run(self):
        self.setup_run()
        time_elapsed = self.adhoc_query_collections()
        self.report_kpi(time_elapsed, op='ad-hoc_query_collections')


class AdHocQueryViewsControlTest(AnalyticsConnectorTest):

    @with_stats
    @timeit
    def adhoc_query_views(self):
        self._exec_analytics_statement(
            (
                'select c.cstate, sum(o.oolcnt) '
                'from bench.ch2.orders_view as o, bench.ch2.customer_view as c '
                'where o.ocid = c.cid '
                'group by c.cstate;'
            ),
            'ad-hoc_query_views.out'
        )

    def run(self):
        self.setup_run()
        time_elapsed = self.adhoc_query_views()
        self.report_kpi(time_elapsed, op='ad-hoc_query_views')


class RestApiControlTestSuite(
    BaseDataExtractionControlTest,
    TabularViewExtractionControlTest,
    AdHocQueryCollectionsControlTest,
    AdHocQueryViewsControlTest
):

    def run(self):
        self.setup_run()

        time_elapsed = self.extract_collection()
        self.report_kpi(time_elapsed, op='extract_collection')

        time_elapsed = self.extract_tabular_view()
        self.report_kpi(time_elapsed, op='extract_tabular_view')

        time_elapsed = self.adhoc_query_collections()
        self.report_kpi(time_elapsed, op='ad-hoc_query_collections')

        time_elapsed = self.adhoc_query_views()
        self.report_kpi(time_elapsed, op='ad-hoc_query_views')


class TableauDataExtractionTest(AnalyticsConnectorTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.tableau_rest = TableauRestHelper(self.test_config, self.analytics_node)
        self.tableau_term = TableauTerminalHelper(self.test_config)
        self.ds_filename = self.test_config.tableau_settings.datasource
        self.ds_name = Path(self.ds_filename).stem
        self.tableau_host = self.test_config.tableau_settings.host

    @with_stats
    def create_and_refresh_extract(self) -> tuple[int, int]:
        create = self.tableau_rest.wait_for_job_complete(
            *self.tableau_rest.create_extract(self.ds_name)
        )
        create_extract_time = self.tableau_rest.get_job_runtime(create)

        refresh = self.tableau_rest.wait_for_job_complete(
            *self.tableau_rest.refresh_extract(self.ds_name)
        )
        refresh_extract_time = self.tableau_rest.get_job_runtime(refresh)

        return create_extract_time, refresh_extract_time

    def run(self):
        self.setup_run()

        try:
            self.tableau_term.run_tsm_command('restart')  # Start Tableau Server
            self.tableau_rest.signin()

            self.tableau_rest.publish_datasource(self.extra_config_path + self.ds_filename)

            create_extract_time, refresh_extract_time = self.create_and_refresh_extract()

            self.report_kpi(create_extract_time, op='create_extract')
            self.report_kpi(refresh_extract_time, op='refresh_extract')

            self.tableau_rest.delete_extract(self.ds_name)
            self.tableau_rest.delete_datasource(self.ds_name)
        finally:
            self.tableau_term.run_tsm_command('stop')  # Stop Tableau Server
