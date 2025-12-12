from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse

import requests
from requests_toolbelt.adapters import socket_options

from logger import logger
from perfrunner.helpers.misc import parse_duration_to_secs, pretty_dict


@dataclass
class CustomAnalyticsQuery:
    statement: str
    named_args: dict = field(default_factory=dict)
    name: Optional[str] = None


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
    with open(filepath, "a") as f:
        f.write(
            pretty_dict(
                {
                    "name": query.name,
                    "statement": query.statement,
                    "named_args": query.named_args,
                    "status_code": status_code,
                    "metrics": body.get("metrics", {}),
                    "plans": body.get("plans", {}),
                    "errors": body.get("errors", []),
                    "warnings": body.get("warnings", []),
                }
            )
            + "\n"
        )


def run_custom_analytics_query_task(
    api_url: str,
    api_auth: tuple[str, str],
    queries: list[CustomAnalyticsQuery],
    log_file_path: Union[str, Path],
    request_params: Optional[dict] = None,
) -> dict:
    """Run the given statements in order, logging metrics and plans for each one.

    Returns {"timings": {query name: {named args: [elapsed secs]}}, "failures": [description]}.
    Statements are run sequentially because a query set may interleave DDL that later queries
    depend on, so any failure is collected and reported rather than aborting mid-set.
    """
    timings = {}
    failures = []

    session = requests.Session()
    parsed_url = urlparse(api_url)
    session.mount(
        f"{parsed_url.scheme}://{parsed_url.netloc}",
        socket_options.TCPKeepAliveAdapter(idle=120, count=20, interval=30),
    )
    session.auth = api_auth
    session.verify = False

    for query in queries:
        display_name = query.name or "(unnamed)"
        logger.info(
            f"Running query {display_name} with named args {query.named_args}:\n{query.statement}"
        )

        payload = {
            "statement": query.statement,
            **query.named_args,
            "optimized-logical-plan": True,
            "plan-format": "STRING",
            "max-warnings": 10,
        } | (request_params or {})

        try:
            resp = session.post(url=api_url, data=payload)
        except requests.RequestException as e:
            # Keep going so a blip doesn't cost us the whole run, but record it as a failure
            logger.error(f"Query {display_name} could not be sent: {e}")
            log_query_to_file(log_file_path, query, None, {})
            failures.append(f"{display_name} ({e.__class__.__name__})")
            continue

        body = decode_response_body(resp)
        log_query_to_file(log_file_path, query, resp.status_code, body)
        if not resp.ok:
            logger.error(f"Query {display_name} failed (HTTP {resp.status_code}): {body}")
            failures.append(f"{display_name} (HTTP {resp.status_code})")
            continue

        if (name := query.name) is not None:
            elapsed_time = parse_duration_to_secs(body.get("metrics", {}).get("elapsedTime", ""))
            logger.info(f"Query {name} elapsed time (s): {elapsed_time:.2f}")
            named_args_str = (
                ",".join(f"{k}={v}" for k, v in query.named_args.items()) or "no_named_args"
            )
            if name not in timings:
                timings[name] = defaultdict(list)
            timings[name][named_args_str].append(elapsed_time)

    return {"timings": timings, "failures": failures}
