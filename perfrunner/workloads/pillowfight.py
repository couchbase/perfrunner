#!/usr/bin/env python

"""Pillowfight: workload generator from libcouchbase."""

import subprocess

from logger import logger


class Pillowfight(object):

    def __init__(self, host, port, bucket, password,
                 num_items=100, num_threads=1, num_iterations=1, writes=50,
                 size=1024):
        self.host = host
        self.port = port
        self.bucket = bucket
        self.password = password
        self.num_items = num_items
        self.num_iterations = num_iterations
        self.num_threads = num_threads
        self.batch_size = 1000
        self.min_item_size = size
        self.max_item_size = size
        # Calculate the number of cycles required for the requested number
        # of iterations across num_items.
        self.num_cycles = (self.num_items / self.batch_size) * num_iterations
        self.set_pcnt = writes

    def run(self):
        # pillowfight performs batch_size operations, cycle_times.
        args = ['cbc-pillowfight',
                '--spec', "couchbase://{host}:{port}/{bucket}".format(
                    host=self.host, port=self.port, bucket=self.bucket),
                '--password', self.password,
                '--batch-size', str(self.batch_size),
                '--num-items', str(self.num_items),
                '--num-threads', str(self.num_threads),
                '--num-cycles', str(self.num_cycles),
                '--min-size', str(self.min_item_size),
                '--max-size', str(self.max_item_size),
                '--set-pct', str(self.set_pcnt),
                # Don't use an explicit populate phase, we just measure "live"
                # load.
                '--no-population']
        try:
            logger.info("Starting Pillowfight as: '" + ' '.join(args) + "'")
            subprocess.check_output(args, stderr=subprocess.STDOUT)
            logger.info("Finished Pillowfight")
        except subprocess.CalledProcessError as e:
            logger.interrupt("Pillowfight failed to run - output: " + e.output)
            raise

if __name__ == '__main__':
    # Small smoketest
    Pillowfight(host='localhost', port='8091', bucket='default',
                password='', num_items=100000, num_threads=4,
                num_iterations=1).run()
