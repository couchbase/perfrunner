import re
from itertools import cycle
from typing import List, Tuple

from couchbase.cluster import QueryOptions, QueryScanConsistency
from couchbase_core.views.params import ViewQuery
from numpy import random


class ViewQueryGen3:

    PARAMS = {
        'limit': 30,
        'stale': 'update_after',
    }

    QUERIES_PER_VIEW = {
        'id_by_city': 9,
        'name_and_email_by_category_and_coins': 6,
        'id_by_realm_and_coins': 5,
        'name_and_email_by_city': 9,
        'name_by_category_and_coins': 6,
        'experts_id_by_realm_and_coins': 5,
        'id_by_realm': 9,
        'achievements_by_category_and_coins': 6,
        'name_and_email_by_realm_and_coins': 5,
        'experts_coins_by_name': 9,
    }

    def __init__(self, ddocs: dict, params: dict):
        self.params = dict(self.PARAMS, **params)

        self.view_sequence = []
        for ddoc_name, ddoc in ddocs.items():
            for view_name in ddoc['views']:
                self.view_sequence += \
                    [(ddoc_name, view_name)] * self.QUERIES_PER_VIEW[view_name]
        random.shuffle(self.view_sequence)
        self.view_sequence = cycle(self.view_sequence)

    @staticmethod
    def generate_params(category: str,
                        city: str,
                        realm: str,
                        name: str,
                        coins: float,
                        **kwargs) -> dict:
        return {
            'id_by_city': {
                'key': city,
            },
            'name_and_email_by_city': {
                'key': city,
            },
            'id_by_realm': {
                'startkey': realm,
            },
            'experts_coins_by_name': {
                'startkey': name,
                'descending': True,
            },
            'name_by_category_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'name_and_email_by_category_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'achievements_by_category_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'id_by_realm_and_coins': {
                'startkey': [realm, coins],
                'endkey': [realm, 10000],
            },
            'name_and_email_by_realm_and_coins': {
                'startkey': [realm, coins],
                'endkey': [realm, 10000],
            },
            'experts_id_by_realm_and_coins': {
                'startkey': [realm, coins],
                'endkey': [realm, 10000],
            },
        }

    def next(self, doc: dict) -> Tuple[str, str, ViewQuery]:
        ddoc_name, view_name = next(self.view_sequence)
        params = self.generate_params(**doc)[view_name]
        params = dict(self.params, **params)
        return ddoc_name, view_name, ViewQuery(**params)


class ViewQueryGenByType3:

    PARAMS = {
        'limit': 20,
        'stale': 'update_after',
    }

    DDOC_NAME = 'ddoc'

    VIEWS_PER_TYPE = {
        'basic': (
            'name_and_street_by_city',
            'name_and_email_by_county',
            'achievements_by_realm',
        ),
        'range': (
            'name_by_coins',
            'email_by_achievement_and_category',
            'street_by_year_and_coins',
        ),
        'group_by': (
            'coins_stats_by_state_and_year',
            'coins_stats_by_gmtime_and_year',
            'coins_stats_by_full_state_and_year',
        ),
        'multi_emits': (
            'name_and_email_and_street_and_achievements_and_coins_by_city',
            'street_and_name_and_email_and_achievement_and_coins_by_county',
            'category_name_and_email_and_street_and_gmtime_and_year_by_country',
        ),
        'compute': (
            'calc_by_city',
            'calc_by_county',
            'calc_by_realm',
        ),
        'body': (
            'body_by_city',
            'body_by_realm',
            'body_by_country',
        ),
        'distinct': (
            'distinct_states',
            'distinct_full_states',
            'distinct_years',
        ),
    }

    def __init__(self, index_type: str, params: dict):
        self.params = dict(self.PARAMS, **params)

        self.view_sequence = cycle(self.VIEWS_PER_TYPE[index_type])

    @staticmethod
    def generate_params(city: dict,
                        county: dict,
                        country: dict,
                        realm: dict,
                        state: dict,
                        full_state: dict,
                        coins: dict,
                        category: str,
                        year: int,
                        achievements: List[int],
                        gmtime: Tuple[int],
                        **kwargs) -> dict:
        return {
            'name_and_street_by_city': {
                'key': city['f']['f'],
            },
            'name_and_email_by_county': {
                'key': county['f']['f'],
            },
            'achievements_by_realm': {
                'key': realm['f'],
            },
            'name_by_coins': {
                'startkey': coins['f'] * 0.5,
                'endkey': coins['f'],
            },
            'email_by_achievement_and_category': {
                'startkey': [0, category],
                'endkey': [achievements[0], category],
            },
            'street_by_year_and_coins': {
                'startkey': [year, coins['f']],
                'endkey': [year, 655.35],
            },
            'coins_stats_by_state_and_year': {
                'key': [state['f'], year],
                'group': 'true'
            },
            'coins_stats_by_gmtime_and_year': {
                'key': [gmtime, year],
                'group_level': 2
            },
            'coins_stats_by_full_state_and_year': {
                'key': [full_state['f'], year],
                'group': 'true'
            },
            'name_and_email_and_street_and_achievements_and_coins_by_city': {
                'key': city['f']['f'],
            },
            'street_and_name_and_email_and_achievement_and_coins_by_county': {
                'key': county['f']['f'],
            },
            'category_name_and_email_and_street_and_gmtime_and_year_by_country': {
                'key': country['f'],
            },
            'calc_by_city': {
                'key': city['f']['f'],
            },
            'calc_by_county': {
                'key': county['f']['f'],
            },
            'calc_by_realm': {
                'key': realm['f'],
            },
            'body_by_city': {
                'key': city['f']['f'],
            },
            'body_by_realm': {
                'key': realm['f'],
            },
            'body_by_country': {
                'key': country['f'],
            },
        }

    def next(self, doc: dict) -> Tuple[str, str, ViewQuery]:
        view_name = next(self.view_sequence)
        params = self.generate_params(**doc)[view_name]
        params = dict(self.params, **params)
        return self.DDOC_NAME, view_name, ViewQuery(**params)


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

        query_opts = QueryOptions(adhoc=bool(ad_hoc),
                                  scan_consistency=self.scan_consistency(scan_consistency),
                                  positional_parameters=args)

        return statement, query_opts
