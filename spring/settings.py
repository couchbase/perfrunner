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
        self.working_set = options.working_set
        self.working_set_access = options.working_set_access
        self.working_set_moving_docs = 0

        self.run_async = options.run_async

        self.workers = options.workers

        # Stubs for library compatibility
        self.reads_and_updates = 0

        self.persist_to = 0
        self.replicate_to = 0

        self.query_workers = 0
        self.query_throughput = 0
        self.index_type = None
        self.ddocs = {}
        self.query_params = {}

        self.ssl_mode = 'none'

        self.n1ql_workers = 0
        self.n1ql_timeout = 0
        self.n1ql_throughput = 0

        self.seq_upserts = False

        self.working_set_move_time = 0

        self.key_fmtr = 'decimal'

        self.power_alpha = 0
        self.zipf_alpha = 0


class TargetSettings:

    def __init__(self, target_uri: str, prefix: str):
        params = urlparse(target_uri)
        if not params.hostname or not params.port or not params.path:
            logger.interrupt('Invalid connection URI')

        self.node = '{}:{}'.format(params.hostname, params.port)
        self.bucket = params.path[1:]
        self.password = params.password or ''
        self.username = params.username or ''
        self.prefix = prefix
