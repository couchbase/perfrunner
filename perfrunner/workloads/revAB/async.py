import random

from couchbase import FMT_UTF8, exceptions
from couchbase._libcouchbase import LCB_NOT_STORED
from couchbase.experimental import enable as enable_experimental
from logger import logger
from twisted.internet import reactor
from txcouchbase.connection import Connection as TxCouchbase

count = 0
enable_experimental()


class AsyncGen(object):

    def __init__(self, iterator, conn):
        self.rng = random.Random(0)
        self.client = TxCouchbase(**conn)
        d = self.client.connect()
        d.addCallback(self.on_connect_success)
        d.addErrback(self.on_connect_error)
        self.iterator = iterator
        self.persons = (_ for _ in iterator)

    def on_connect_error(self, err):
        logger.info('Got error: {}'.format(err))
        # Handle it, it's a normal Failure object
        self.client._close()
        err.trap()

    def on_connect_success(self, _):
        logger.info('Couchbase Connected!')
        self.process_next_person()

    def process_next_person(self):
        """Pick the next person from the graph; and add them to CB"""
        try:
            person = self.persons.next()
        except StopIteration:
            logger.info('StopIteration')
            reactor.stop()
            return
        value = self.iterator.person_to_value(self.rng, person)
        # Build list of append ops for multi_append
        ops = {}
        for friend in self.iterator.graph[person]:
            key = self.iterator.person_to_key(friend)
            ops[key] = ';' + value
        # Do the actual work.
        d = self.client.append_multi(ops, format=FMT_UTF8)
        d.addCallback(self._on_append)
        d.addErrback(self._on_multi_fail, ops)

    def _on_append(self, result):
        """Success, schedule next"""
        global count
        count += 1
        self.process_next_person()

    def _on_multi_fail(self, err, ops):
        """Multi failed, crack and handle failures with set."""
        err.trap(exceptions.NotStoredError, exceptions.TimeoutError)
        if err.check(exceptions.NotStoredError):
            # One or more keys do not yet exist, handle with set
            for k, v in err.value.all_results.items():
                logger.info('VAL: {}'.format(err.value))
                if not v.success:
                    if v.rc == LCB_NOT_STORED:
                        # Snip off semicolon for initial value.
                        logger.info('SET: {} {}'.format(k, ops[k][1:]))
                        d = self.client.set(k, ops[k][1:], format=FMT_UTF8)
                        d.addCallback(self._on_set)
                        d.addErrback(self._on_set_fail)

        elif err == exceptions.TimeoutError:
            logger.interrupt('Timeout: {}'.format(err))
        else:
            logger.interrupt('Unhandled error: {}'.format(err))

    def _on_set(self, result):
        pass

    def _on_set_fail(self, err):
        logger.interrupt('ON_SET_FAIL'.format(err))
