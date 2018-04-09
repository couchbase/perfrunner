from threading import Thread
from typing import Iterator, List

from perfrunner.helpers.local import run_kvgen

NUM_KVGEN_INSTANCES = 5


def kvgen(master_node: str, num_docs: int, wait: bool):
    docs_per_instances = num_docs // NUM_KVGEN_INSTANCES

    threads = []
    for thread in create_kvgen_instances(master_node, docs_per_instances):
        threads.append(thread)
        thread.start()

    if wait:
        wait_for_kvgen_threads(threads)


def create_kvgen_instances(master_node: str, num_docs: int) -> Iterator[Thread]:
    for i in range(NUM_KVGEN_INSTANCES):
        key_prefix = 'prefix-{}'.format(i)
        yield Thread(target=run_kvgen, args=(master_node, num_docs, key_prefix))


def wait_for_kvgen_threads(threads: List[Thread]):
    for t in threads:
        t.join()
