import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from typing import Iterator, Optional

import numpy

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.rest import RestType
from perfrunner.workloads.bigfun.query_gen import Query, new_queries


def store_metrics(query: Query, response_json: dict):
    with open("bigfun.log", "a") as fh:
        fh.write(
            pretty_dict(
                {
                    "id": query.id,
                    "description": query.description,
                    "statement": query.statement,
                    "metrics": response_json.get("metrics", {}),
                    "plans": response_json.get("plans", {}),
                    "errors": response_json.get("errors", {}),
                }
            )
        )
        fh.write("\n")


def run_concurrent_queries(
    rest: RestType,
    nodes: list[str],
    query: Query,
    concurrency: int,
    num_requests: int,
    request_params: Optional[dict] = None,
) -> list[float]:
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        nodes = cycle(nodes)

        def run_query(node: str, query: Query, params: Optional[dict] = None) -> float:
            t0 = time.time()
            response = rest.exec_analytics_statement(node, query.statement, params)
            latency = time.time() - t0  # Latency in seconds
            store_metrics(query, response.json())
            return latency

        futures = [
            executor.submit(run_query, next(nodes), query, request_params)
            for _ in range(num_requests)
        ]
        timings = []
        for future in as_completed(futures):
            timings.append(future.result())
        return timings


def bigfun(
    rest: RestType,
    nodes: list[str],
    concurrency: int,
    num_requests: int,
    query_set: str,
    request_params: Optional[dict] = None,
) -> Iterator:
    logger.info("Running BigFun queries")

    if num_requests <= 0:
        logger.info(f"{num_requests=}, no BigFun queries will be run.")
        return

    if concurrency <= 0:
        logger.info(f"{concurrency=}, no BigFun queries will be run.")
        return

    for query in new_queries(query_set):
        timings = run_concurrent_queries(
            rest, nodes, query, concurrency, num_requests, request_params
        )
        avg_latency = int(1000 * numpy.mean(timings))  # Latency in ms
        yield query, avg_latency
