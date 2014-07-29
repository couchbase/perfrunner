# Simulator for a 'reverse' phone book - keys are ~all phone numbers in the
# world, values are lists of phone books which contain that phone number.

# Approach: Use NetworkX to build a example representative social network.
# Iterate all nodes (i.e. users) in the graph and for each vertex (friend phone
# number in their AB) perform an `append(friend_tel, user_tel)`.

import random
from threading import Thread
from multiprocessing import Pool

from logger import logger
from twisted.internet import reactor

from perfrunner.workloads.revAB.async import AsyncGen
from perfrunner.workloads.revAB.sync import SyncGen
from perfrunner.workloads.revAB.graph import generate_graph, PersonIterator

USERS = 10000
ITERATIONS = 3
WORKERS = 8
ENGINE = 'threaded'

done = 0


def produce_AB(iterator):
    # Repeatedly:
    # 1. Populate the graph into Couchbase; randomly delete a percentage of all
    # documents during population.
    logger.info('START: {}'.format(iterator.start))
    gen = SyncGen(iterator)
    for i in range(ITERATIONS):
        if iterator.start == 0:
            # Show progress (but just for 1 thread to avoid spamming)
            logger.info('Iteration {}/{}'.format(i + 1, ITERATIONS))
        gen.populate()

    logger.info('END: {}'.format(iterator.start))

    global done
    done += 1
    if done == WORKERS:
        gen.report_summary()


def main():
    # Seed RNG with a fixed value to give deterministic runs
    random.seed(0)
    graph = generate_graph(USERS)
    graph_keys = graph.nodes()
    random.shuffle(graph_keys)

    if ENGINE == 'twisted':
        for start in range(WORKERS):
            AsyncGen(iterator=PersonIterator(graph, graph_keys, start, WORKERS))
        # Then drive the event loop
        reactor.run()

    elif ENGINE == 'threaded':
        threads = list()
        for start in range(WORKERS):
            iterator = PersonIterator(graph, graph_keys, start, WORKERS)
            t = Thread(target=produce_AB, args=(iterator, ))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    elif ENGINE == 'multiprocessing':
        iterators = [
            PersonIterator(graph, graph_keys, start, WORKERS)
            for start in range(WORKERS)
        ]
        pool = Pool(processes=WORKERS)
        pool.map(produce_AB, iterators)

    else:
        produce_AB(0)


if __name__ == '__main__':
    main()
