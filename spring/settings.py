from urlparse import urlparse

from logger import logger


class WorkloadSettings(object):

    def __init__(self, options):
        self.creates = options.creates
        self.reads = options.reads
        self.updates = options.updates
        self.deletes = options.deletes
        self.cases = 0  # Stub for library compatibility

        self.ops = options.ops
        self.throughput = options.throughput

        self.doc_gen = options.generator
        self.size = options.size
        self.items = options.items
        self.expiration = options.expiration
        self.working_set = options.working_set
        self.working_set_access = options.working_set_access

        self.workers = options.workers
        self.query_workers = 0  # Stub for library compatibility
        self.dcp_workers = 0  # Stub for library compatibility

        self.index_type = None
        self.ddocs = {}
        self.qparams = {}
        self.n1ql = False
        self.n1ql_workers = 0

        self.async = options.async


class TargetSettings(object):

    def __init__(self, target_uri, prefix):
        params = urlparse(target_uri)
        if not params.hostname or not params.port or not params.path:
            logger.interrupt('Invalid connection URI')

        self.node = '{}:{}'.format(params.hostname, params.port)
        self.bucket = params.path[1:]
        self.password = params.password or ''
        self.prefix = prefix
