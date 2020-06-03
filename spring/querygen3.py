import re
from itertools import cycle
from typing import List, Tuple

from couchbase.cluster import QueryOptions, QueryScanConsistency


class N1QLQueryGen3:

    def __init__(self, queries: List[dict]):
        queries = [
            (query['statement'], query['args'], query.get('scan_consistency'), query.get('ad_hoc'))
            for query in queries
        ]
        self.queries = cycle(queries)

    def generate_query(self):
        return

    def scan_consistency(self, val):
        if val == 'request_plus':
            return QueryScanConsistency.REQUEST_PLUS
        elif val == 'not_bound':
            return QueryScanConsistency.NOT_BOUNDED
        else:
            return QueryScanConsistency.NOT_BOUNDED

    def next(self, key: str, doc: dict, replace_targets: dict=None) -> Tuple[str, QueryOptions]:
        statement, args, scan_consistency, ad_hoc = next(self.queries)
        if replace_targets:
            for bucket in replace_targets.keys():
                bucket_substring = "`{}`".format(bucket)
                for i in range(statement.count(bucket_substring)):
                    where = [m.start() for m in re.finditer(bucket_substring, statement)][i]
                    before = statement[:where]
                    after = statement[where:]
                    scope, collection = replace_targets[bucket][i].split(":")
                    replace_target = "default:`{}`.`{}`.`{}`".format(bucket, scope, collection)
                    after = after.replace(bucket_substring, replace_target)
                    statement = before + after
        if 'key' in args:
            args = [key]
        else:
            args = args.format(**doc)
            args = eval(args)

        query_opts = QueryOptions(ad_hoc=bool(ad_hoc),
                                  scan_consistency=self.scan_consistency(scan_consistency),
                                  positional_parameters=args)

        return statement, query_opts
