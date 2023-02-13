import csv
import random
import time
from typing import Optional, Union

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

    def update(self, operation: str, value: Union[float, tuple[float, float]],
               target: Optional[str] = None):
        """Conditionally add new measurements to the reservoir."""
        if not value:  # Ignore bad results
            return

        self.count += 1
        timestamp = int(time.time() * 10 ** 9)  # Nanosecond granularity

        if isinstance(value, float):
            latency_single, latency_total = value, None
        else:
            latency_single, latency_total = value

        measurement = (operation, timestamp, latency_single, latency_total, target)

        if len(self.values) < self.capacity:
            self.values.append(measurement)
        else:
            r = int(self.count * random.random())
            if r < self.capacity:
                self.values[r] = measurement

    def dump(self, filename: str):
        """Write all measurements to a local CSV file."""
        logger.info('Writing measurements to {}'.format(filename))
        with open(filename, 'w') as fh:
            writer = csv.writer(fh)
            for measurement in self.values:
                writer.writerow(measurement)
