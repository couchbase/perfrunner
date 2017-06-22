# Simulator for a 'reverse' phone book - keys are ~all phone numbers in the
# world, values are lists of phone books which contain that phone number.

# Approach: Use NetworkX to build a example representative social network.
# Iterate all nodes (i.e. users) in the graph and for each vertex (friend phone
# number in their AB) perform an `append(friend_tel, user_tel)`.

import random
from multiprocessing import Pool
from optparse import OptionParser
from threading import Thread

from logger import logger
from twisted.internet import reactor

from perfrunner.workloads.revAB.async import AsyncGen
from perfrunner.workloads.revAB.graph import PersonIterator, generate_graph
from perfrunner.workloads.revAB.sync import SyncGen

done = 0


def produce_ab(iterator, iterations, conn):
    # Repeatedly:
    # 1. Populate the graph into Couchbase; randomly delete a percentage of all
    # documents during population.
    logger.info('START: {}'.format(iterator.start))
    gen = SyncGen(iterator, conn)
    for i in range(iterations):
        if iterator.start == 0:
            # Show progress (but just for 1 thread to avoid spamming)
            logger.info('Iteration {}/{}'.format(i + 1, iterations))
        gen.populate()

    logger.info('END: {}'.format(iterator.start))

    global done
    done += 1
    if done == iterator.step:
        gen.report_totals()


def get_options():
    usage = '%prog -c cluster -t test_config'

    parser = OptionParser(usage)

    parser.add_option('-e', dest='engine', default='threaded')
    parser.add_option('-i', dest='iterations', type='int', default=3)
    parser.add_option('-u', dest='users', type='int', default=10000)
    parser.add_option('-w', dest='workers', type='int', default=8)

    parser.add_option('-H', dest='host', default='127.0.0.1')
    parser.add_option('-p', dest='port', type='int', default=8091)
    parser.add_option('-b', dest='bucket', default='default')

    options, _ = parser.parse_args()
    return options


def main():
    options = get_options()

    # Seed RNG with a fixed value to give deterministic runs
    random.seed(0)
    graph = generate_graph(options.users)
    graph_keys = graph.nodes()
    random.shuffle(graph_keys)

    conn = {
        'host': options.host, 'port': options.port, 'bucket': options.bucket,
    }

    if options.engine == 'twisted':
        for start in range(options.workers):
            AsyncGen(iterator=PersonIterator(graph,
                                             graph_keys,
                                             start,
                                             options.workers),
                     conn=conn)
        # Then drive the event loop
        reactor.run()

    elif options.engine == 'threaded':
        threads = list()
        for start in range(options.workers):
            iterator = PersonIterator(graph, graph_keys, start, options.workers)
            t = Thread(
                target=produce_ab,
                args=(iterator, options.iterations, conn),
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    elif options.engine == 'multiprocessing':
        results = list()
        pool = Pool(processes=options.workers)
        for start in range(options.workers):
            r = pool.apply_async(
                func=produce_ab,
                args=(
                    PersonIterator(graph, graph_keys, start, options.workers),
                    options.iterations,
                    conn,
                )
            )
            results.append(r)
        for r in results:
            r.get()


if __name__ == '__main__':
    main()
