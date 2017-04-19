from urllib.parse import urlparse

from logger import logger


class WorkloadSettings:

    def __init__(self, options):
        self.creates = options.creates
        self.reads = options.reads
        self.updates = options.updates
        self.deletes = options.deletes

        self.ops = options.ops
        self.throughput = options.throughput

        self.doc_gen = options.generator
        self.size = options.size
        self.items = options.items
        self.expiration = options.expiration
        self.working_set = options.working_set
        self.working_set_access = options.working_set_access

        self.async = options.async

        self.workers = options.workers

        # Stubs for library compatibility
        self.query_workers = 0
        self.n1ql_workers = 0

        self.fts_config = None
        self.fts_updates_swap = 0
        self.fts_updates_reverse = 0

        self.index_type = None
        self.ddocs = {}
        self.query_params = {}

        self.use_ssl = False

        self.working_set_move_time = 0
        self.hash_keys = False


class TargetSettings:

    def __init__(self, target_uri, prefix):
        params = urlparse(target_uri)
        if not params.hostname or not params.port or not params.path:
            logger.interrupt('Invalid connection URI')

        self.node = '{}:{}'.format(params.hostname, params.port)
        self.bucket = params.path[1:]
        self.password = params.password or ''
        self.prefix = prefix
