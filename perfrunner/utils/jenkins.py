import glob
import json
from collections import defaultdict
from typing import Dict, Iterator, List, Tuple

import jenkins
from couchbase.bucket import Bucket
from couchbase.exceptions import KeyExistsError

from logger import logger

JobMapping = Dict[str, List[Dict[str, str]]]


class JenkinsScanner:

    COUCHBASE_BUCKET = 'jenkins'

    COUCHBASE_HOST = 'perflab.sc.couchbase.com'

    COUCHBASE_PASSWORD = 'password'  # Yay!

    JENKINS_URL = 'http://perf.jenkins.couchbase.com'

    def __init__(self):
        self.bucket = self.new_bucket()
        self.jenkins = jenkins.Jenkins(self.JENKINS_URL)
        self.jobs = set()

    @property
    def connection_string(self) -> str:
        return 'couchbase://{}/{}?password={}'.format(self.COUCHBASE_HOST,
                                                      self.COUCHBASE_BUCKET,
                                                      self.COUCHBASE_PASSWORD)

    def new_bucket(self) -> Bucket:
        return Bucket(connection_string=self.connection_string)

    def store_build_info(self, attributes: dict):
        try:
            self.bucket.insert(key=attributes['url'], value=attributes)
            logger.info('Added: {}'.format(attributes['url']))
        except KeyExistsError:
            logger.info('Skipped: {}'.format(attributes['url']))

    def map_jobs(self) -> JobMapping:
        job_mapping = defaultdict(list)

        for pipeline in glob.glob('tests/pipelines/weekly-*.json'):
            with open(pipeline) as fh:
                for component, jobs in json.load(fh).items():
                    for job in jobs:
                        self.jobs.add(job['job'])
                        job_mapping[component].append(job)

        return job_mapping

    def map_test_configs(self, job_mapping: JobMapping) -> Dict[str, str]:
        test_configs = {}

        for component, jobs in job_mapping.items():
            for job in jobs:
                test_config = job['test_config']
                test_configs[test_config] = component.split('-')[0]

        return test_configs

    @staticmethod
    def extract_parameters(actions: List[Dict]) -> dict:
        for action in actions:
            if action.get('_class') == 'hudson.model.ParametersAction':
                parameters = {}
                for parameter in action['parameters']:
                    parameter_name = parameter['name']
                    if parameter_name == 'dry_run' and parameter['value']:
                        return {}  # Ignore dry runs
                    if parameter_name in ('cluster', 'test_config', 'version'):
                        parameters[parameter_name] = parameter['value']
                return parameters

    @staticmethod
    def merge_attributes(component: str,
                         job: str,
                         build_info: dict,
                         build_parameters: dict) -> dict:
        build_parameters.update({
            'component': component,
            'job': job,
            'success': build_info['result'] == 'SUCCESS',
            'timestamp': build_info['timestamp'],
            'url': build_info['url'],
        })
        return build_parameters

    def build_info(self) -> Iterator[Tuple[str, dict]]:
        for job_name in self.jobs:
            job_info = self.jenkins.get_job_info(job_name,
                                                 fetch_all_builds=True)
            for build in job_info['builds']:
                build_number = build['number']
                yield job_name, self.jenkins.get_build_info(job_name,
                                                            build_number)

    def build_ext_info(self) -> Iterator[Tuple[str, dict, dict]]:
        for job_name, build_info in self.build_info():
            build_actions = build_info['actions']
            build_parameters = self.extract_parameters(build_actions)

            if build_parameters:
                yield job_name, build_info, build_parameters

    def run(self):
        jobs = self.map_jobs()
        test_configs = self.map_test_configs(jobs)

        for job_name, build_info, build_parameters in self.build_ext_info():
            test_config = build_parameters['test_config']
            component = test_configs.get(test_config)

            if component is not None:
                attributes = self.merge_attributes(component,
                                                   job_name,
                                                   build_info,
                                                   build_parameters)
                self.store_build_info(attributes)


def main():
    scanner = JenkinsScanner()
    scanner.run()


if __name__ == '__main__':
    main()
