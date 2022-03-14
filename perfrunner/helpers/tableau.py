from datetime import datetime
from pathlib import Path
from time import sleep
from xml.etree import ElementTree as ETree

import requests
from fabric.api import execute, local, put, run
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

from logger import logger
from perfrunner.settings import TestConfig


class TableauTerminalHelper:
    def __init__(self, test_config: TestConfig):
        self.test_config = test_config
        self.host = self.test_config.tableau_settings.host
        self.connector_vendor = self.test_config.tableau_settings.connector_vendor

    def _run_command(self, cmd: str, capture=False):
        if self.host == 'localhost':
            return local(cmd, capture=capture, shell='/bin/bash')
        else:
            return execute(lambda: run(cmd), hosts=[self.host])

    def _get_cdata_tableau_connector_version(self):
        cmd = ("java -jar /opt/cdata/lib/cdata.tableau.couchbase.jar -version | "
               "grep -oP \"Build: (\\d+\\.)+\\d+\" | "
               "cut -d ' ' -f 2")
        return self._run_command(cmd, capture=True)

    def run_tsm_command(self, cmd: str, capture=False):
        logger.info('Running: tsm {}'.format(cmd))
        full_cmd = ('tsm_dir=( /opt/tableau/tableau_server/packages/customer-bin.* ) && '
                    '"${{tsm_dir[0]}}"/tsm {}'.format(cmd))
        return self._run_command(full_cmd, capture=capture)

    def get_connector_version(self):
        if self.connector_vendor == 'couchbase':
            return self.test_config.client_settings.tableau_connector
        elif self.connector_vendor == 'cdata':
            return self._get_cdata_tableau_connector_version()

    def copy_file(self, file: str, destination: str):
        if self.host == 'localhost':
            local('cp {} {}'.format(file, destination))
        else:
            execute(lambda: put(file, destination), hosts=[self.host])


class TableauRestHelper:
    POLL_INTERVAL = 5

    CREATE_EXTRACT_JOB = 'create_extracts'
    REFRESH_EXTRACT_JOB = 'refresh_extracts'

    SIGNIN_REQUEST_BODY = (
        '<tsRequest>\n'
        '    <credentials name="{username}" password="{password}">\n'
        '        <site contentUrl="" />\n'
        '    </credentials>\n'
        '</tsRequest>\n'
    )

    PUBLISH_REQUEST_PAYLOAD = (
        '<tsRequest>\n'
        '    <datasource name="{ds_name}"\n'
        '     description="Datasource for Tableau Connector perf. tests">\n'
        '        <connectionCredentials name="Administrator"\n'
        '         password="password"\n'
        '         embed="true" />\n'
        '    </datasource>\n'
        '</tsRequest>\n'
    )

    def __init__(self, test_config: TestConfig, analytics_node: str):
        self.host = test_config.tableau_settings.host
        self.api_version = test_config.tableau_settings.api_version
        self.credentials = test_config.tableau_settings.credentials
        self.analytics_node = analytics_node

        self.site_id = None
        self.token = None
        self.datasource_id = None
        self.datasources = {}
        self.ns = '{http://tableau.com/api}'

    def _auth_header(self):
        return {'X-Tableau-Auth': self.token}

    def _api_request(self, command, endpoint, **kwargs) -> requests.Response:
        if 'headers' not in kwargs:
            kwargs['headers'] = self._auth_header()
        else:
            kwargs['headers'].update(self._auth_header())

        url = 'http://{}/api/{}/sites/{}{}'.format(self.host,
                                                   self.api_version,
                                                   self.site_id,
                                                   endpoint)

        response = requests.request(command, url, **kwargs)

        if not response.ok:
            raise Exception(response.content)

        return response

    def api_get(self, endpoint: str, **kwargs) -> requests.Response:
        return self._api_request('GET', endpoint, **kwargs)

    def api_post(self, endpoint: str, **kwargs) -> requests.Response:
        return self._api_request('POST', endpoint, **kwargs)

    def api_delete(self, endpoint: str, **kwargs) -> requests.Response:
        return self._api_request('DELETE', endpoint, **kwargs)

    def signin(self):
        request_body = self.SIGNIN_REQUEST_BODY.format(**self.credentials)
        url = 'http://{}/api/{}/auth/signin'.format(self.host, self.api_version)
        response = requests.post(url, data=request_body)
        tree = ETree.fromstring(response.content)
        self.token = tree.find('.//{}credentials'.format(self.ns)).attrib['token']
        self.site_id = tree.find('.//{}site'.format(self.ns)).attrib['id']

    def fetch_datasources(self):
        response = self.api_get('/datasources')
        tree = ETree.fromstring(response.content)
        for ds in tree.findall('.//{}datasource'.format(self.ns)):
            self.datasources[ds.attrib['name']] = ds.attrib['id']

    def publish_datasource(self, datasource_file: str):
        logger.info('Tableau Server: publishing datasource from file {}'.format(datasource_file))
        ds_path = Path(datasource_file)
        ds_template_path = Path(datasource_file + '.template')

        files = {
            'request_payload': (
                None,
                self.PUBLISH_REQUEST_PAYLOAD.format(ds_name=ds_path.stem),
                'text/xml'
            ),
            'tableau_datasource': (
                ds_path.name,
                open(ds_template_path, 'r').read().format(server=self.analytics_node),
                'application/octet-stream'
            )
        }

        fields = []
        for name, (filename, contents, mimetype) in files.items():
            rf = RequestField(name=name, data=contents, filename=filename)
            rf.make_multipart(content_type=mimetype)
            fields.append(rf)

        request_body, content_type = encode_multipart_formdata(fields)
        boundary = content_type.split(';')[-1]
        headers = {'Content-Type': 'multipart/mixed; {}'.format(boundary)}

        response = self.api_post('/datasources?overwrite=true',
                                 data=request_body,
                                 headers=headers)
        tree = ETree.fromstring(response.content)

        ds = tree.find('.//{}datasource'.format(self.ns))
        self.datasources[ds.attrib['name']] = ds.attrib['id']

    def delete_datasource(self, datasource_name: str):
        logger.info('Tableau Server: deleting datasource {}'.format(datasource_name))
        datasource_id = self.datasources[datasource_name]
        endpoint = '/datasources/{}'.format(datasource_id)

        response = self.api_delete(endpoint)

        if not response.ok:
            del self.datasources[datasource_name]

    def create_extract(self, datasource_name: str) -> tuple[str, str, str]:
        logger.info('Tableau Server: creating extract for datasource {}'.format(datasource_name))
        datasource_id = self.datasources[datasource_name]
        endpoint = '/datasources/{}/createExtract'.format(datasource_id)

        response = self.api_post(endpoint)
        tree = ETree.fromstring(response.content)

        job = tree.find('.//{}job'.format(self.ns))
        return (
            job.attrib['id'], self.CREATE_EXTRACT_JOB, job.attrib['createdAt']
        )

    def refresh_extract(self, datasource_name: str) -> tuple[str, str, str]:
        logger.info('Tableau Server: refreshing extract for datasource {}'.format(datasource_name))
        datasource_id = self.datasources[datasource_name]
        endpoint = '/datasources/{}/refresh'.format(datasource_id)

        request_body = "<tsRequest></tsRequest>"
        response = self.api_post(endpoint, data=request_body)
        tree = ETree.fromstring(response.content)

        job = tree.find('.//{}job'.format(self.ns))
        return (
            job.attrib['id'], self.REFRESH_EXTRACT_JOB, job.attrib['createdAt']
        )

    def delete_extract(self, datasource_name: str):
        logger.info('Tableau Server: deleting extract for datasource {}'.format(datasource_name))
        datasource_id = self.datasources[datasource_name]
        endpoint = '/datasources/{}/deleteExtract'.format(datasource_id)

        response = self.api_post(endpoint)

        if not response.ok:
            raise Exception('Failed to delete extract for datasource {}'
                            .format(datasource_name))

    def get_job_direct(self, job_id: str) -> ETree.Element:
        endpoint = '/jobs/{}'.format(job_id)

        response = self.api_get(endpoint)

        tree = ETree.fromstring(response.content)
        job = tree.find('.//{}job'.format(self.ns))
        return job

    def _get_jobs(self, job_type: str = None, created_since: str = None) -> ETree.Element:
        filters = []

        if job_type:
            filters.append('jobType:eq:{}'.format(job_type))

        if created_since:
            filters.append('createdAt:gte:{}'.format(created_since))

        query_filter = '?filter={}'.format(','.join(filters)) if filters else ''

        endpoint = '/jobs' + query_filter

        response = self.api_get(endpoint)
        tree = ETree.fromstring(response.content)
        return tree

    def get_job(self, job_id: str,
                job_type: str = None,
                created_since: str = None) -> ETree.Element:
        if job_type == self.REFRESH_EXTRACT_JOB:
            job = self.get_job_direct(job_id)
        else:
            tree = self._get_jobs(job_type=job_type, created_since=created_since)
            job = tree.find(".//{}backgroundJob[@id='{}']".format(self.ns, job_id))
        return job

    def get_jobs(self, job_type: str = None, created_since: str = None) -> list[ETree.Element]:
        tree = self._get_jobs(job_type=job_type, created_since=created_since)
        jobs = tree.findall('.//{}backgroundJob'.format(self.ns))
        return jobs

    def wait_for_job_complete(self, job_id: str,
                              job_type: str = None,
                              created_at: str = None) -> ETree.Element:
        sleep(self.POLL_INTERVAL)
        done_attrs = {'completedAt', 'endedAt'}

        # While the job attributes don't include any of the done_attrs, poll the job
        while not (job := self.get_job(job_id, job_type, created_at)).attrib.keys() & done_attrs:
            logger.info('Waiting for job to finish...')
            sleep(self.POLL_INTERVAL)

        if ('status' in job.attrib and job.attrib['status'] != 'Success') or \
           ('finishCode' in job.attrib and job.attrib['finishCode'] != '0'):
            raise Exception('Job failed: {}'.format(ETree.tostring(job, encoding='unicode')))

        return job

    @staticmethod
    def get_job_runtime(job: ETree.Element) -> int:
        started_at = job.attrib['startedAt']
        completed_at = job.attrib.get('completedAt', job.attrib.get('endedAt'))

        datetime_format = "%Y-%m-%dT%H:%M:%SZ"
        start_time = datetime.strptime(started_at, datetime_format)
        end_time = datetime.strptime(completed_at, datetime_format)

        runtime = (end_time - start_time).seconds
        return runtime
