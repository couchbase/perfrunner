import json
from argparse import ArgumentParser
from collections import defaultdict
from multiprocessing import set_start_method
from typing import Dict, List

import requests

set_start_method("fork")


class Weekly:

    BASE_URL = 'http://showfast.sc.couchbase.com/weekly/api/v1'

    @property
    def builds(self):
        url = '{}/builds'.format(self.BASE_URL)
        return requests.get(url).json()

    def update_status(self, status: dict):
        url = '{}/status'.format(self.BASE_URL)
        requests.post(url, json.dumps(status))


def count_jobs(pipelines: List[str]) -> Dict[str, int]:
    counter = defaultdict(int)
    for pipeline in pipelines:
        with open(pipeline) as fh:
            for component, jobs in json.load(fh).items():
                if jobs:
                    component = component.split('-')[0]
                    counter[component] += len(jobs)
    return counter


def init_build(build: str, jobs: Dict[str, int]):
    weekly = Weekly()

    for component, num_jobs in jobs.items():
        status = {
            'build': build,
            'component': component,
            'test_status': {
                'total': num_jobs,
            },
        }
        weekly.update_status(status)


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-v', '--version', required=True)
    parser.add_argument('pipelines', type=str, nargs='+')

    return parser.parse_args()


def main():
    args = get_args()

    jobs = count_jobs(args.pipelines)

    init_build(build=args.version, jobs=jobs)


if __name__ == '__main__':
    main()
