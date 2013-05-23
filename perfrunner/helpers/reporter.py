import time

from logger import logger


class Reporter(object):

    def start(self):
        self.ts = time.time()

    def finish(self, action):
        elapsed = time.time() - self.ts
        logger.info(
            'Time taken to perform {0}: {1:.1f} sec'.format(action, elapsed)
        )
