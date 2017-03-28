from optparse import OptionParser
from queue import LifoQueue
from typing import Iterator, List

import requests
from logger import logger

BASE_URL = 'http://showfast.sc.couchbase.com'


def get_menu() -> dict:
    return requests.get(url=BASE_URL + '/static/menu.json').json()


def get_benchmarks(component: str, category: str) -> List[dict]:
    api = '/api/v1/benchmarks/{}/{}'.format(component, category)
    return requests.get(url=BASE_URL + api).json() or []


def hide_benchmark(benchmark_id: str):
    api = '/api/v1/benchmarks/{}'.format(benchmark_id)
    requests.patch(url=BASE_URL + api)


def showfast_iterator(components: List[str]) -> Iterator:
    for component, meta in get_menu()['components'].items():
        if component in components:
            for category in meta['categories']:
                yield component, category['id']


def parse_release(build: str) -> str:
    return build.split('-')[0]


def benchmark_iterator(components: List[str], max_builds: int) -> Iterator:
    for component, category in showfast_iterator(components=components):
        curr_metric, curr_release = None, None
        queue = LifoQueue(maxsize=max_builds)

        for benchmark in get_benchmarks(component, category):
            if not benchmark['hidden']:
                release = parse_release(benchmark['build'])

                if curr_metric != benchmark['metric']:
                    curr_metric, curr_release = benchmark['metric'], release
                    queue.queue.clear()

                if release != curr_release:
                    curr_release = release
                    queue.queue.clear()

                if queue.full():
                    yield benchmark
                else:
                    queue.put(benchmark)


def hide(components: List[str], max_builds: int):
    for b in benchmark_iterator(components=components, max_builds=max_builds):
        logger.info('Hiding: build={build}, metric={metric}'.format(**b))
        hide_benchmark(b['id'])


def main():
    parser = OptionParser()

    parser.add_option('-c', '--components', dest='components', default=[],
                      type='str', help='comma separated list of components')
    parser.add_option('-m', '--max-builds', dest='max_builds', default=8,
                      type='int', help='maximum number of builds per release')

    options, args = parser.parse_args()

    hide(components=options.components.split(','),
         max_builds=options.max_builds)


if __name__ == '__main__':
    main()
