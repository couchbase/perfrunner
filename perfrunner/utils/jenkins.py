import glob
import json
import os
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Tuple, Union

import jenkins
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException, DocumentNotFoundException
from couchbase.options import ClusterOptions, QueryOptions

from logger import logger
from perfrunner.utils.weekly import Weekly

JobMapping = Dict[str, List[Dict[str, str]]]


class BaseScanner:
    COUCHBASE_HOST = "cb.2ioc8okhsq7qiudd.cloud.couchbase.com"
    COUCHBASE_USER = os.getenv("CAPELLA_PERFLAB_USER")
    COUCHBASE_PASSWORD = os.getenv("CAPELLA_PERFLAB_KEY")
    RETRY_LIMIT = 3

    def __init__(self, bucket_name: str):
        options = ClusterOptions(
            authenticator=PasswordAuthenticator(
                self.COUCHBASE_USER, self.COUCHBASE_PASSWORD, cert_path="/root/capella_perflab.pem"
            )
        )
        options.apply_profile("wan_development")
        self.cluster = Cluster(
            self.connection_string,
            options,
        )
        self.bucket = self.cluster.bucket(bucket_name).default_collection()
        self.weekly = Weekly()

    @property
    def connection_string(self) -> str:
        return f"couchbases://{self.COUCHBASE_HOST}?sasl_mech_force=PLAIN"

    def upsert_to_bucket(self, key: str, value: Union[int, dict] = {}):
        count = 0
        while count < self.RETRY_LIMIT:
            try:
                self.bucket.upsert(key=key, value=value)
                return
            except CouchbaseException as ex:
                count += 1
                logger.warn(f"Failed adding value (Retry count {count}/{self.RETRY_LIMIT}). {ex}")

    def get_checkpoint(self, key: str, default: Any = {}) -> Any:
        """Do a get on a bucket and return the content if found.

        The returned content will be of the same type as the provided default value.
        """
        try:
            return self.bucket.get(key).content_as[type(default)]
        except DocumentNotFoundException:
            # For jobs checkpoints we want to return the 0 default. But for metrics checkpoints
            # we specifically check for None since the stored checkpoints are empty dictionaries.
            return default if isinstance(default, int) else None
        except Exception as ex:
            logger.error(ex)
        return default

class JenkinsScanner(BaseScanner):
    JENKINS_URL = "http://perf.jenkins.couchbase.com"

    STATUS_QUERY = """
        SELECT component,
               COUNT(CASE WHEN (success = true) THEN 1 ELSE NULL END) AS passed,
               COUNT(CASE WHEN (success = false) THEN 1 ELSE NULL END) AS failed
        FROM jenkins
        WHERE version = $1
          AND success IS NOT NULL
        GROUP BY component;
    """

    BUILD_QUERY = """
        SELECT component, test_config, `cluster`, url
        FROM jenkins
        WHERE version = $1;
    """

    def __init__(self):
        super().__init__("jenkins")
        self.jenkins = jenkins.Jenkins(self.JENKINS_URL)
        self.jobs = set()

    def store_build_info(self, attributes: dict):
        key = self.generate_key(attributes)
        self.upsert_to_bucket(key=key, value=attributes)

    @staticmethod
    def generate_key(attributes: dict) -> str:
        return '_'.join((attributes['cluster'],
                         attributes['test_config'],
                         attributes['version']))

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
            'duration': build_info['duration'],
            'job': job,
            'success': build_info['result'] == 'SUCCESS',
            'timestamp': build_info['timestamp'],
            'url': build_info['url'],
        })
        return build_parameters

    def build_info(self) -> Iterator[Tuple[str, dict]]:
        for job_name in self.jobs:
            checkpoint: int = self.get_checkpoint(job_name, 0)
            new_checkpoint = checkpoint

            job_info = self.jenkins.get_job_info(job_name, fetch_all_builds=True)

            for build in sorted(job_info['builds'], key=lambda b: b['number']):
                build_number = build['number']
                if build_number > checkpoint:
                    build_info = self.jenkins.get_build_info(job_name,
                                                             build_number)
                    if build_info['result'] is not None:
                        new_checkpoint = max(new_checkpoint, build_number)
                        yield job_name, build_info

            self.upsert_to_bucket(key=job_name, value=new_checkpoint)
            logger.info(f"Added checkpoint for {job_name}")

    def build_ext_info(self) -> Iterator[Tuple[str, dict, dict]]:
        for job_name, build_info in self.build_info():
            build_actions = build_info['actions']
            build_parameters = self.extract_parameters(build_actions)

            if build_parameters:
                yield job_name, build_info, build_parameters

    def scan(self):
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

    def update_status(self):
        for build in self.weekly.builds:
            logger.info('Updating status of build {}'.format(build))

            for status in self.cluster.query(
                self.STATUS_QUERY, QueryOptions(positional_parameters=[build])
            ):
                status = {
                    'build': build,
                    'component': status['component'],
                    'test_status': {
                        'passed': status['passed'],
                        'failed': status['failed'],
                    },
                }
                self.weekly.update_status(status)

    def find_builds(self, version: str) -> Iterator[dict]:
        for build in self.cluster.query(
            self.BUILD_QUERY, QueryOptions(positional_parameters=[version])
        ):
            yield build


def main():
    scanner = JenkinsScanner()
    scanner.scan()
    scanner.update_status()


if __name__ == '__main__':
    main()
