
import concurrent.futures
import logging
import sys
from argparse import ArgumentParser
from datetime import timedelta
from time import perf_counter_ns, sleep

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.collection import CBCollection
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, UpsertOptions


class ConfigPush:

    def __init__(self, coll: CBCollection, keys: list[str], threads: int, timeout: int):
        self.coll = coll
        self.threads = threads
        self.keys = keys
        self.done = False
        self.upsert_timeout = timeout

    def config_push_task(self, thread: int):
        key_index = 0
        doc = SimpleDocument()
        first_error = None
        next_success = None
        exception_msg = None
        while True:
            try:
                key = self.keys[(key_index := ((key_index + 1) % len(self.keys)))]
                self.coll.upsert(key, doc.next(key),
                                 UpsertOptions(timeout=timedelta(milliseconds=self.upsert_timeout)))
                if first_error:
                    next_success = perf_counter_ns()
                    break
            except Exception as e:
                if not first_error:
                    first_error = perf_counter_ns()
                    exception_msg = e

        time_taken = next_success - first_error
        logging.info('[FIRST FAILURE] {}'.format(first_error))
        logging.info('[NEXT SUCCESS] {}'.format(next_success))
        logging.info('[TIME TAKEN (s)] {}'.format(round(time_taken/1e9, 2)))
        logging.info('Exception: {}'.format(exception_msg))

    def run(self):
        self.config_push_task(0)

    def run_with_threading(self, runtime: int):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
            executor.map(self.config_push_task, range(self.threads))

            sleep(runtime)
            self.done = True


class SimpleDocument:

    @staticmethod
    def hex_digest(key: str) -> str:
        return '%032x' % key.__hash__()

    @staticmethod
    def build_email(alphabet: str) -> str:
        return '%s@%s.com' % (alphabet[12:18], alphabet[18:24])

    @staticmethod
    def build_string(alphabet: str):
        return '%s %s' % (alphabet[16:24], alphabet[24:])

    def next(self, key: str) -> dict:
        alphabet = self.hex_digest(key) + self.hex_digest(key[::-1])
        return {
            'name': self.build_string(alphabet),
            'email': self.build_email(alphabet),
            'count': ord(alphabet[-1])
        }


def get_args():
    parser = ArgumentParser()

    parser.add_argument('--host', default='127.0.0.1',
                        help='the host string/ip')
    parser.add_argument('--bucket', required=True,
                        help='bucket name')
    parser.add_argument('--username', default='Administrator',
                        help='cluster username')
    parser.add_argument('--password', default='password',
                        help='password')
    parser.add_argument('--num-threads',
                        dest='threads',
                        default=1,
                        help='threads to use')
    parser.add_argument('--timeout',
                        default=2500,
                        help='kv timeout (ms)')
    parser.add_argument('--keys', required=True,
                        help='keys to process')
    parser.add_argument('--time',
                        default=10,
                        help='the time to run the experiment (s)')
    parser.add_argument('--c-poll-interval',
                        dest='cp_interval',
                        default=2500,
                        help='ms')
    parser.add_argument('extras',
                        nargs='*',
                        help='extra settings will be ignored')
    return parser.parse_known_args()


def main():
    args, _ = get_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format='%(asctime)s.%(msecs)03d, %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    c_options = ClusterOptions(PasswordAuthenticator(args.username, args.password),
                               timeout_options=ClusterTimeoutOptions(
                                   kv_timeout=timedelta(milliseconds=int(args.timeout))),
                               config_poll_interval=timedelta(milliseconds=int(args.cp_interval)))
    cluster = Cluster('couchbase://{}'.format(args.host), c_options)
    cluster.wait_until_ready(timedelta(seconds=5))

    cb_coll = cluster.bucket(args.bucket).default_collection()
    threads = int(args.threads)
    test = ConfigPush(cb_coll, args.keys.split(','), threads, int(args.timeout))
    if threads > 1:
        test.run_with_threading()
    else:
        test.run()


if __name__ == '__main__':
    main()
