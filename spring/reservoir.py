import csv
import random
import time

from logger import logger


class Reservoir:

    """Implement Algorithm R.

    See also https://www.cs.umd.edu/~samir/498/vitter.pdf
    """

    MAX_CAPACITY = 10 ** 5

    def __init__(self, num_workers: int = 1):
        self.capacity = self.MAX_CAPACITY // num_workers
        self.values = []
        self.count = 0  # Total items to sample

    def update(self, operation, value):
        """Conditionally add new measurements to the reservoir."""
        if not value:  # Ignore bad results
            return

        self.count += 1
        timestamp = int(time.time() * 10 ** 9)  # Nanosecond granularity

        if len(self.values) < self.capacity:
            self.values.append((operation, timestamp, value))
        else:
            r = int(self.count * random.random())
            if r < self.capacity:
                self.values[r] = (operation, timestamp, value)

    def dump(self, filename: str):
        """Write all measurements to a local CSV file."""
        logger.info('Writing measurements to {}'.format(filename))
        with open(filename, 'w') as fh:
            writer = csv.writer(fh)
            for line in self.values:
                operation = line[0]
                timestamp = line[1]
                value = line[2]
                if isinstance(value, tuple):
                    value_a = value[0]
                    value_b = value[1]
                    writer.writerow([operation, timestamp, value_a, value_b])
                else:
                    writer.writerow([operation, timestamp, value])
