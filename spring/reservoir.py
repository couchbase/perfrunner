import csv
import random
import time

from logger import logger


class Reservoir:

    """
    Implementation of Algorithm R.

        https://www.cs.umd.edu/~samir/498/vitter.pdf

    """

    MAX_CAPACITY = 10 ** 5

    def __init__(self, num_workers=1):
        self.capacity = self.MAX_CAPACITY / num_workers
        self.values = []
        self.count = 0  # Total items to sample

    def update(self, value):
        """Conditionally add new measurements to the reservoir."""
        if not value:  # Ignore bad results
            return

        self.count += 1
        timestamp = int(time.time() * 10 ** 9)  # Nanosecond granularity

        if len(self.values) < self.capacity:
            self.values.append((timestamp, value))
        else:
            r = int(self.count * random.random())
            if r < self.capacity:
                self.values[r] = (timestamp, value)

    def dump(self, filename):
        """Write all measurements to a local CSV file."""
        logger.info('Writing measurements to {}'.format(filename))
        with open(filename, 'w') as fh:
            writer = csv.writer(fh)
            for timestamp, value in self.values:
                writer.writerow([timestamp, value])
