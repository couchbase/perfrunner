import itertools
import json
import re
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import NamedTuple, Optional, Union
from urllib.parse import urlparse

import requests
from requests_toolbelt.adapters import socket_options

from logger import logger
from perfrunner.helpers.misc import parse_duration_to_secs, pretty_dict

# Keys expand_query_configs understands on a query config entry. Anything else is a typo
# (e.g. "plan_contain") that would otherwise silently disable the check it was meant to add.
#
# The statement sent for an entry is "(SET <clause>;)* <prefix>? <statement> <suffix>?", with
# prefix, statement and suffix joined by single spaces.
#
# - statement (required): the query text.
# - prefix / suffix: text joined before / after statement.
# - set: one SET clause (a string) or a list of them. Each becomes its own "SET <clause>;"
#   ahead of the rest of the statement, in order.
# - named_args: [{name, values}]. The entry expands once per combination of every arg's values
#   (a cartesian product over all the named_args entries).
# - name: if set, every execution of the entry is timed via the response's server-side
#   elapsedTime and reported as a KPI, one metric per (name, named args) combination, using
#   the median across all its samples. If not set, the entry still runs (so it can create or
#   drop state, or warm caches) but is never timed or reported.
# - repeat: emit the entry this many times (default 1). It is the outer loop around
#   named_args, so repeat: 2 with two named_args values runs value1, value2, value1, value2.
# - plan_contains / plan_excludes: physical operator names (a string or a list of them) the
#   response plan must or must not contain. Checked on every execution; any mismatch fails
#   the run before a KPI is posted, so a KPI can never be reported against the wrong
#   execution path.
# - log_results: if true, the response's `results` are also written to the query log. Off by
#   default, since an unnamed warmup or probe entry can return a very large result set.
# - once: if true, the entry runs in the first pass only (see the [test_case] option `passes`
#   in CustomQueryTest).
# - concurrency: send this many copies of the entry at once (default 1). Every copy's elapsed
#   time is a sample under the entry's name, and every copy is plan-checked; the set still
#   advances one entry at a time.
# - plan_order: substrings (a string or a list of them) that must appear in this order in the
#   response plan, which is printed top-down, e.g. a function call before a join operator,
#   meaning the call is evaluated on the join's output. Checked like plan_contains.
KNOWN_QUERY_CONFIG_KEYS = {
    "statement",
    "prefix",
    "suffix",
    "set",
    "named_args",
    "name",
    "repeat",
    "plan_contains",
    "plan_excludes",
    "log_results",
    "once",
    "concurrency",
    "plan_order",
}

# Keys a top-level `ratios` entry understands. Each is reported as the median of the numerator
# entry's samples over the denominator's.
KNOWN_RATIO_KEYS = {"name", "title", "numerator", "denominator", "chirality"}

# Default per-request timeout. requests.post() blocks forever without an explicit timeout, so a
# query that never gets a response would hang the run. Analytics queries can legitimately run
# for a long time, so this is generous and can be raised per test.
DEFAULT_REQUEST_TIMEOUT_SECS = 3600

UNNAMED_QUERY = "(unnamed)"
NO_NAMED_ARGS = "no_named_args"


class QueryKpi(NamedTuple):
    name: str
    args_label: str
    metric_id_suffix: str
    median: float


@dataclass
class CustomAnalyticsQuery:
    statement: str
    named_args: dict = field(default_factory=dict)
    name: Optional[str] = None
    plan_contains: list[str] = field(default_factory=list)
    plan_excludes: list[str] = field(default_factory=list)
    log_results: bool = False
    plan_order: list[str] = field(default_factory=list)
    concurrency: int = 1


def _as_list(value) -> list:
    """Treat a single string value as a one-element list, otherwise copy the list."""
    return [value] if isinstance(value, str) else list(value)


def _expand_one_config(index: int, query_conf: dict) -> tuple[bool, list[CustomAnalyticsQuery]]:
    """Expand a single query config entry into its (repeat x named_args) queries.

    Returns (once, queries): once is whether the entry is restricted to the first pass.
    """
    unknown_keys = set(query_conf) - KNOWN_QUERY_CONFIG_KEYS
    if unknown_keys:
        raise ValueError(
            f"Unknown key(s) {sorted(unknown_keys)} in query config entry {index} "
            f"({query_conf.get('name', UNNAMED_QUERY)})"
        )

    statement = " ".join(
        (
            query_conf.get("prefix", ""),
            query_conf["statement"],
            query_conf.get("suffix", ""),
        )
    ).strip()

    if set_clauses := query_conf.get("set"):
        statement = "".join(f"SET {clause}; " for clause in _as_list(set_clauses)) + statement

    named_arg_combinations = [{}]
    if named_args_conf := query_conf.get("named_args"):
        arg_names = [arg["name"] for arg in named_args_conf]
        arg_values = [arg["values"] for arg in named_args_conf]
        named_arg_combinations = [
            dict(zip(arg_names, values)) for values in itertools.product(*arg_values)
        ]

    if (concurrency := int(query_conf.get("concurrency", 1))) < 1:
        raise ValueError(
            f"concurrency must be at least 1 in query config entry {index} "
            f"({query_conf.get('name', UNNAMED_QUERY)}), got {concurrency}"
        )

    queries = [
        CustomAnalyticsQuery(
            statement,
            named_args,
            query_conf.get("name"),
            _as_list(query_conf.get("plan_contains", [])),
            _as_list(query_conf.get("plan_excludes", [])),
            bool(query_conf.get("log_results", False)),
            _as_list(query_conf.get("plan_order", [])),
            concurrency,
        )
        for _ in range(int(query_conf.get("repeat", 1)))
        for named_args in named_arg_combinations
    ]
    return bool(query_conf.get("once", False)), queries


def expand_query_configs(configs: list[dict], passes: int = 1) -> list[CustomAnalyticsQuery]:
    """Expand a yaml query-config list into the flat, ordered list a run executes.

    Each entry becomes `repeat` copies (default 1) crossed with every combination of its
    `named_args` values, in that order: repeat is the outer loop, so `repeat: 2` with two
    named_args values runs value1, value2, value1, value2. The whole expanded list is then
    repeated `passes` times (default 1), except entries marked `once: true`, which appear in
    the first pass only.

    Raises ValueError if an entry has a key this function does not recognise.
    """
    entries = [_expand_one_config(i, query_conf) for i, query_conf in enumerate(configs)]

    expanded = []
    for pass_num in range(passes):
        for once, queries in entries:
            if not once or pass_num == 0:
                expanded.extend(queries)
    return expanded


def split_query_conf(doc: Union[list, dict]) -> tuple[list[dict], list[dict]]:
    """Split a loaded query yaml into (query entries, ratio definitions).

    The top level is either a bare list of query entries, or a mapping with a `queries` list
    and an optional `ratios` list.
    """
    if isinstance(doc, list):
        return doc, []
    if unknown := set(doc) - {"queries", "ratios"}:
        raise ValueError(f"Unknown top-level key(s) {sorted(unknown)} in query config")
    return doc.get("queries", []), doc.get("ratios", [])


def validate_ratios(configs: list[dict], ratios: list[dict]):
    """Fail on a ratio that cannot be computed from the queries the set runs.

    Both sides must name an entry without named_args: a named-args entry is several different
    queries under one name, and pooling them into one median would mix them.
    """
    timed = {c["name"]: c for c in configs if c.get("name")}
    for ratio in ratios:
        if unknown := set(ratio) - KNOWN_RATIO_KEYS:
            raise ValueError(f"Unknown key(s) {sorted(unknown)} in ratio {ratio.get('name')}")
        if missing := KNOWN_RATIO_KEYS - {"chirality"} - set(ratio):
            raise ValueError(f"Missing key(s) {sorted(missing)} in ratio {ratio.get('name')}")
        for side in ("numerator", "denominator"):
            if (query_name := ratio[side]) not in timed:
                raise ValueError(
                    f"Ratio {ratio['name']} {side} {query_name} is not a named query entry"
                )
            if timed[query_name].get("named_args"):
                raise ValueError(
                    f"Ratio {ratio['name']} {side} {query_name} has named_args, which a ratio "
                    "cannot reduce to one value"
                )


def compute_ratios(timings: dict, ratios: list[dict]) -> list[tuple[dict, float]]:
    """Return (ratio definition, median(numerator) / median(denominator)) for each ratio."""

    def median_of(name: str) -> float:
        return statistics.median(timings[name][NO_NAMED_ARGS])

    return [
        (ratio, median_of(ratio["numerator"]) / median_of(ratio["denominator"]))
        for ratio in ratios
    ]


def plan_check_failures(query: CustomAnalyticsQuery, body: dict) -> list[str]:
    """Check one response's plan against the query's plan_contains/excludes/order.

    Returns a description per violated expectation (empty if the query has no plan checks
    configured, or every expectation holds). A check configured against a response that carries
    no plan at all (e.g. optimized-logical-plan was overridden to false) is itself a failure,
    since a silently-skipped check is worse than a loud one.
    """
    if not (query.plan_contains or query.plan_excludes or query.plan_order):
        return []

    display_name = query.name or UNNAMED_QUERY
    plans = body.get("plans")
    if not plans:
        return [f"{display_name} (no plan in response to check)"]

    plan_text = json.dumps(plans)
    failures = [
        f"{display_name} (plan does not contain {operator})"
        for operator in query.plan_contains
        if operator not in plan_text
    ]
    failures += [
        f"{display_name} (plan contains {operator})"
        for operator in query.plan_excludes
        if operator in plan_text
    ]

    # Every occurrence of each entry must come before the first occurrence of the next one.
    for earlier, later in zip(query.plan_order, query.plan_order[1:]):
        if earlier not in plan_text or later not in plan_text:
            failures.append(f"{display_name} (plan lacks {earlier} or {later})")
        elif plan_text.rindex(earlier) > plan_text.index(later):
            failures.append(f"{display_name} (plan has {earlier} below {later})")
    return failures


def reduce_timings_to_kpis(timings: dict) -> list[QueryKpi]:
    """Reduce {name: {named_args_str: [elapsed secs]}} to one row per (name, named args).

    Each row is a QueryKpi: args_label is " (k=v,...)", or empty when there were no named args
    (the NO_NAMED_ARGS key); metric_id_suffix is "{name}_{named_args_str}" (just "{name}" for
    NO_NAMED_ARGS), lowercased with every run of non-alphanumeric characters collapsed to a
    single underscore -- the raw named-args string contains "$", "=" and ".", none of which
    belong in a metric id.
    """
    rows = []
    for name, by_args in timings.items():
        for args_str, samples in by_args.items():
            if args_str == NO_NAMED_ARGS:
                args_label, raw_id = "", name
            else:
                args_label, raw_id = f" ({args_str})", f"{name}_{args_str}"
            metric_id_suffix = re.sub(r"[^a-z0-9]+", "_", raw_id.lower()).strip("_")
            rows.append(QueryKpi(name, args_label, metric_id_suffix, statistics.median(samples)))
    return rows


def decode_response_body(resp: requests.Response) -> dict:
    """Decode a response body as JSON, tolerating the non-JSON bodies errors can carry."""
    try:
        return resp.json()
    except ValueError:
        logger.warning(f"Non-JSON response body (HTTP {resp.status_code}): {resp.text[:500]}")
        return {}


def log_query_to_file(
    filepath: Union[str, Path],
    query: CustomAnalyticsQuery,
    status_code: Optional[int],
    body: dict,
):
    entry = {
        "name": query.name,
        "statement": query.statement,
        "named_args": query.named_args,
        "status_code": status_code,
        "metrics": body.get("metrics", {}),
        "plans": body.get("plans", {}),
        "errors": body.get("errors", []),
        "warnings": body.get("warnings", []),
    }
    # Opt-in: an unnamed warmup/probe entry can return a very large result set.
    if query.log_results:
        entry["results"] = body.get("results", [])

    with open(filepath, "a") as f:
        f.write(pretty_dict(entry) + "\n")


def _new_session(api_url: str, api_auth: tuple[str, str]) -> requests.Session:
    session = requests.Session()
    parsed_url = urlparse(api_url)
    session.mount(
        f"{parsed_url.scheme}://{parsed_url.netloc}",
        socket_options.TCPKeepAliveAdapter(idle=120, count=20, interval=30),
    )
    session.auth = api_auth
    session.verify = False
    return session


def _run_one(
    session: requests.Session,
    api_url: str,
    query: CustomAnalyticsQuery,
    log_file_path: Union[str, Path],
    request_params: Optional[dict],
    request_timeout: int,
    log_lock: Lock,
) -> tuple[Optional[float], list[str]]:
    """Run one statement, returning (elapsed secs, failure descriptions).

    Errors are returned rather than raised, so one bad statement does not cost the rest of the
    set. The elapsed time is None when the request itself failed.
    """
    display_name = query.name or UNNAMED_QUERY

    payload = {
        "statement": query.statement,
        **query.named_args,
        "optimized-logical-plan": True,
        "plan-format": "STRING",
        "max-warnings": 10,
    } | (request_params or {})

    try:
        resp = session.post(url=api_url, data=payload, timeout=request_timeout)
    except requests.RequestException as e:
        # Keep going so a blip doesn't cost us the whole run, but record it as a failure
        logger.error(f"Query {display_name} could not be sent: {e}")
        with log_lock:
            log_query_to_file(log_file_path, query, None, {})
        return None, [f"{display_name} ({e.__class__.__name__})"]

    body = decode_response_body(resp)
    with log_lock:
        log_query_to_file(log_file_path, query, resp.status_code, body)
    if not resp.ok:
        logger.error(f"Query {display_name} failed (HTTP {resp.status_code}): {body}")
        return None, [f"{display_name} (HTTP {resp.status_code})"]

    failures = plan_check_failures(query, body)
    for reason in failures:
        logger.error(f"Plan check failed: {reason}")

    elapsed_time = parse_duration_to_secs(body.get("metrics", {}).get("elapsedTime", ""))
    logger.info(f"Query {display_name} elapsed time (s): {elapsed_time:.2f}")
    return elapsed_time, failures


def run_custom_analytics_query_task(
    api_url: str,
    api_auth: tuple[str, str],
    queries: list[CustomAnalyticsQuery],
    log_file_path: Union[str, Path],
    request_params: Optional[dict] = None,
    request_timeout: int = DEFAULT_REQUEST_TIMEOUT_SECS,
) -> dict:
    """Run the given statements in order, logging metrics and plans for each one.

    Returns {"timings": {query name: {named args: [elapsed secs]}}, "failures": [description]}.
    Statements are run sequentially because a query set may interleave DDL that later queries
    depend on, so any failure is collected and reported rather than aborting mid-set. A query
    with concurrency > 1 sends that many copies at once and records each one's elapsed time;
    the set still advances one entry at a time.
    """
    timings = {}
    failures = []
    log_lock = Lock()
    session = _new_session(api_url, api_auth)

    def run_on(s: requests.Session, query: CustomAnalyticsQuery):
        return _run_one(
            s, api_url, query, log_file_path, request_params, request_timeout, log_lock
        )

    for query in queries:
        display_name = query.name or UNNAMED_QUERY
        logger.info(
            f"Running query {display_name} with named args {query.named_args} at "
            f"concurrency {query.concurrency}:\n{query.statement}"
        )

        if query.concurrency == 1:
            results = [run_on(session, query)]
        else:
            # A session per thread: requests.Session is not documented as thread-safe, and
            # sharing one would serialise the very contention this run is trying to measure.
            sessions = [_new_session(api_url, api_auth) for _ in range(query.concurrency)]
            with ThreadPoolExecutor(max_workers=query.concurrency) as pool:
                results = list(pool.map(lambda s, q=query: run_on(s, q), sessions))
            for s in sessions:
                s.close()

        for _, run_failures in results:
            failures.extend(run_failures)

        if (name := query.name) is None:
            continue

        named_args_str = (
            ",".join(f"{k}={v}" for k, v in query.named_args.items()) or NO_NAMED_ARGS
        )
        if name not in timings:
            timings[name] = defaultdict(list)
        timings[name][named_args_str].extend(
            elapsed for elapsed, _ in results if elapsed is not None
        )

    return {"timings": timings, "failures": failures}
