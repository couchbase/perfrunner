import json
import random
from datetime import datetime
from typing import Iterator, List

import dateutil.parser as parser

from perfrunner.helpers.misc import human_format

MIN_DATE = "2000-01-01T00:00:00"
MAX_DATE = "2014-08-29T23:59:59"

STATEMENTS = {
    'BF03': 'SELECT VALUE u '
            'FROM `GleambookUsers-{}` u '
            'WHERE u.user_since >= "{}" AND u.user_since < "{}";',
    'BF04': 'SELECT VALUE u '
            'FROM `GleambookUsers-{}` u '
            'WHERE u.user_since >= "{}" AND u.user_since < "{}" '
            'AND (SOME e IN u.employment SATISFIES e.end_date IS UNKNOWN);',
    'BF08': 'SELECT cm.user.screen_name AS username, AVG(LENGTH(cm.message_text)) AS avg '
            'FROM `ChirpMessages-{}` cm '
            'WHERE cm.send_time >= "{}" AND cm.send_time < "{}" '
            'GROUP BY cm.user.screen_name '
            'ORDER BY avg '
            'LIMIT 10;',
    'BF10': 'SELECT VALUE cm '
            'FROM `ChirpMessages-{}` cm '
            'WHERE (SOME e IN cm.employment SATISFIES e.doesnt_exist IS NOT UNKNOWN);',
    'BF11': 'SELECT DISTINCT VALUE 1 '
            'FROM (SELECT * FROM `ChirpMessages-{}` cm ORDER BY send_time ) AS foo;',
    'BF14': 'SELECT META(u).id AS id, COUNT(*) AS count '
            'FROM `GleambookUsers-{}` u, `GleambookMessages-{}` m '
            'WHERE TO_STRING(META(u).id) = m.author_id '
            'AND u.user_since >= "{}" AND u.user_since < "{}" '
            'AND m.send_time >= "{}" AND m.send_time < "{}" '
            'GROUP BY META(u).id;',
    'BF15': 'SELECT META(u).id AS id, COUNT(*) AS count '
            'FROM `GleambookUsers-{}` u, `GleambookMessages-{}` m '
            'WHERE TO_STRING(META(u).id) = m.author_id '
            'AND u.user_since >= "{}" AND u.user_since < "{}" '
            'AND m.send_time >= "{}" AND m.send_time < "{}" '
            'GROUP BY META(u).id '
            'ORDER BY count '
            'LIMIT 10;',
    'BF16': 'SELECT u.id AS id, COUNT(*) AS count '
            'FROM `GleambookUsers-{}` u, `GleambookMessages-{}` m '
            'WHERE u.id = m.author_id '
            'AND u.user_since >= "{}" AND u.user_since < "{}" '
            'AND m.send_time >= "{}" AND m.send_time < "{}" '
            'GROUP BY u.id;',
    'WF01': 'set `compiler.windowmemory` "4MB"; '
            'SELECT subqry.id, subqry.wf FROM '
            '(SELECT u.id AS id, ROW_NUMBER() '
            'OVER(PARTITION BY meta(u).id) AS wf '
            'FROM `GleambookUsers-{}` u) subqry WHERE id < 100',
    'WF02': 'set `compiler.windowmemory` "4MB"; '
            'SELECT subqry.id, subqry.wf FROM '
            '(SELECT u.id AS id, NTILE(3) '
            'OVER(PARTITION BY SUBSTR(u.user_since, 0, 10)) AS wf '
            'FROM `GleambookUsers-{}` u) subqry WHERE id < 100',
    'WF03': 'set `compiler.windowmemory` "4MB"; '
            'SELECT subqry.id, subqry.wf FROM '
            '(SELECT u.id AS id, NTILE(3) '
            'OVER(PARTITION BY SUBSTR(u.user_since, 6, 4)) AS wf '
            'FROM `GleambookUsers-{}` u) subqry WHERE id < 100',
    'WF04': 'set `compiler.windowmemory` "4MB"; '
            'SELECT subqry.id, subqry.wf FROM '
            '(SELECT u.id AS id, AVG(ARRAY_COUNT(u.friend_ids))'
            ' OVER(PARTITION BY  SUBSTR(u.user_since, 6, 4) ORDER BY id '
            'RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS wf '
            'FROM `GleambookUsers-{}` u) subqry WHERE id < 100',
    'WF05': 'set `compiler.windowmemory` "4MB"; '
            'SELECT subqry.id, subqry.wf FROM '
            '(SELECT u.id as id, SUM(ARRAY_COUNT(u.friend_ids)) '
            'OVER(PARTITION BY  SUBSTR(u.user_since, 6, 4) ORDER BY id '
            'RANGE BETWEEN UNBOUNDED PRECEDING AND 0 FOLLOWING) AS wf '
            'FROM `GleambookUsers-{}` u) subqry WHERE id < 100',
}

DESCRIPTIONS = {
    'BF03': 'Temporal range scan ({} matches)',
    'BF04': 'Existential quantification ({} matches)',
    'BF08': 'Top-K ({} matches)',
    'BF10': 'Full scan',
    'BF11': 'Full sort',
    'BF14': 'Select join with grouping aggregation ({} matches)',
    'BF15': 'Select join with Top-K ({} matches)',
    'BF16': 'Select join with grouping aggregation ({} matches)',
    'WF01': 'Minimal streaming window function',
    'WF02': 'Minimal window function with materialized partition in memory',
    'WF03': 'Minimal window function with materialized partition spilling to disk',
    'WF04': 'Windowed aggregate with small frame',
    'WF05': 'Windowed aggregate with unbounded preceding frame',
}


def iso2seconds(dt: str) -> int:
    return int(parser.parse(dt).strftime('%s'))


def seconds2iso(s: int) -> str:
    return datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S')


def min_timestamp() -> int:
    return iso2seconds(MIN_DATE)


def max_timestamp() -> int:
    return iso2seconds(MAX_DATE)


def interval() -> int:
    return max_timestamp() - min_timestamp()


def items_per_second(dataset: str, num_set: int) -> float:
    return {
        'ChirpMessages': 2e8 / num_set / interval(),
        'GleambookMessages': 1e8 / num_set / interval(),
        'GleambookUsers': 2e7 / num_set / interval(),
    }[dataset]


def new_offset(seconds: int) -> int:
    return random.randint(min_timestamp(), max_timestamp() - seconds)


def new_dates(dataset: str, num_matches: float, num_set: int) -> List[str]:
    seconds = int(num_matches / items_per_second(dataset, num_set))
    offset = new_offset(seconds)
    return [seconds2iso(offset), seconds2iso(offset + seconds)]


def bf03params(num_matches: float, num_set: int) -> List[str]:
    set_index = random.randrange(1, num_set+1)
    return list(str(set_index)) + new_dates('GleambookUsers', num_matches, num_set)


def bf04params(num_matches: float, num_set: int) -> List[str]:
    return bf03params(num_matches, num_set)


def bf08params(num_matches: float, num_set: int) -> List[str]:
    set_index = random.randrange(1, num_set + 1)
    return list(str(set_index)) + new_dates('ChirpMessages', num_matches, num_set)


def bf10params(num_set: int) -> List[str]:
    set_index = random.randrange(1, num_set + 1)
    return list(str(set_index))


def bf11params(num_set: int) -> List[str]:
    return bf10params(num_set)


def bf14params(num_matches: float, num_set: int) -> List[str]:
    set_index = str(random.randrange(1, num_set + 1))
    return [set_index, set_index] + \
        new_dates('GleambookUsers', num_matches, num_set) + \
        new_dates('GleambookMessages', num_matches, num_set)


def bf15params(num_matches: float, num_set: int) -> List[str]:
    return bf14params(num_matches, num_set)


def bf16params(num_matches: float, num_set: int) -> List[str]:
    return bf14params(num_matches, num_set)


def wf01params(num_set: int) -> List[str]:
    return bf10params(num_set)


def wf02params(num_set: int) -> List[str]:
    return bf10params(num_set)


def wf03params(num_set: int) -> List[str]:
    return bf10params(num_set)


def wf04params(num_set: int) -> List[str]:
    return bf10params(num_set)


def wf05params(num_set: int) -> List[str]:
    return bf10params(num_set)


def new_params(qid: str, num_matches: float, num_set: int) -> List[str]:
    return {
        'BF03': bf03params(num_matches, num_set),
        'BF04': bf04params(num_matches, num_set),
        'BF08': bf08params(num_matches, num_set),
        'BF10': bf10params(num_set),
        'BF11': bf11params(num_set),
        'BF14': bf14params(num_matches, num_set),
        'BF15': bf15params(num_matches, num_set),
        'BF16': bf16params(num_matches, num_set),
        'WF01': wf01params(num_set),
        'WF02': wf02params(num_set),
        'WF03': wf03params(num_set),
        'WF04': wf04params(num_set),
        'WF05': wf05params(num_set),
    }[qid]


def new_statement(qid: str, num_matches: float, num_set: int) -> str:
    params = new_params(qid, num_matches, num_set)
    return STATEMENTS[qid].format(*params)


def new_description(qid: str, num_matches: float) -> str:
    template = DESCRIPTIONS[qid]
    return template.format(human_format(num_matches))


class Query:

    def __init__(self, qid: str, num_matches: float, num_set: int):
        self.id = qid
        self.num_matches = num_matches
        self.num_set = num_set

    @property
    def statement(self) -> str:
        return new_statement(self.id, self.num_matches, self.num_set)

    @property
    def description(self) -> str:
        return new_description(self.id, self.num_matches)


def new_queries(query_set: str) -> Iterator[Query]:
    with open(query_set) as fh:
        queries = json.load(fh)

    for query in queries:
        for num_matches in query['matches']:
            yield Query(query['id'], num_matches, query['set'])
