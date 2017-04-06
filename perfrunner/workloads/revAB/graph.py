import itertools

from logger import logger

from perfrunner.workloads.revAB.fittingCode import socialModels


def generate_graph(users):
    logger.info('Generating graph')
    graph = socialModels.nearestNeighbor_mod(users, 0.90, 5)
    logger.info(
        'Done: {} nodes, {} edges.'
        .format(graph.number_of_nodes(), graph.number_of_edges())
    )
    return graph


class PersonIterator:

    def __init__(self, graph, graph_keys, start, step):
        self.graph = graph
        self.graph_keys = graph_keys
        self.start = start
        self.step = step

    @staticmethod
    def person_to_key(person):
        # Pad p to typical tel number length (12 chars).
        return u'{:012}'.format(person)

    @staticmethod
    def person_to_value(rng, person):
        # Should be same as key, but to speed up memory increase use a random
        # length between 12 and 400 chars.
        width = rng.randint(12, 400)
        return u'{:0{width}}'.format(person, width=width)

    def __iter__(self):
        for k in itertools.islice(self.graph_keys, self.start, None, self.step):
            yield k
