import time
from uuid import uuid4

from couchbase import Couchbase
from logger import logger


class Reporter(object):

    def start(self):
        self.ts = time.time()

    def finish(self, action):
        elapsed = time.time() - self.ts
        logger.info(
            'Time taken to perform "{0}": {1:.1f} sec'.format(action, elapsed)
        )
        return elapsed

    def post(self, host, data):
        key = uuid4().hex
        try:
            cb = Couchbase.connect(host=host, bucket='benchmarks')
            cb.set(key, data)
        except Exception, e:
            logger.warn('Failed to post results, {0}'.format(e))
        else:
            logger.info('Successfully posted: {0}'.format(data))
