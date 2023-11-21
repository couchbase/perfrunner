import numpy as np

from logger import logger
from perfrunner.helpers.rest import RestHelper


class VectorRecallCalculator:

    def __init__(self, settings, rest: RestHelper) -> None:
        self.settings = settings
        self.rest = rest

    def query_and_calculate_recall_and_accuracy(self, query, indexName,
                                                groundTruth) -> tuple[float, int]:
        searchResult = self.rest.fts_search_query(self.settings.fts_master_node, indexName, query)
        k = query["knn"][0]["k"]
        if searchResult["total_hits"] > 0:
            id_arr = [hit["id"] for hit in searchResult["hits"]]
            recall = sum(x in id_arr[:k] for x in groundTruth[:k])/float(k)
            return recall, int(id_arr[0] == groundTruth[0])
        logger.error(f"Not found any hits for query:\n{query}")
        return 0, 0

    def create_query(self, vectorlist, fts_raw_query_map, field_name, k=3) -> dict:
        query = {}
        for key in fts_raw_query_map.keys():
            query[key] = fts_raw_query_map[key]
        query["limit"] = k
        query["knn"] = [{
            "k": k,
            "field": field_name,
            "vector": vectorlist
            }]
        return query

    def run(self) -> tuple[float, float]:
        logger.info("Querying the vectors and calculating recall and accuracy")
        test_data_file = self.settings.test_data_file[1:]
        fts_raw_query_map = self.settings.fts_raw_query_map
        field_name = self.settings.test_query_field
        k_nearest_neighbour = int(self.settings.k_nearest_neighbour)
        indexName = list(self.settings.fts_index_map.keys())[0]
        test_file = open(test_data_file, 'r')
        if self.settings.ground_truth_file_name:
            ground_truth = [x.split() for x in
                            open(self.settings.ground_truth_file_name, 'r').readlines()]
        else:
            logger.interrupt("GroundTruth file not found. Necessary for recall")
        recall = []
        accuracy = []
        for vector, truth in zip(test_file.readlines(), ground_truth):
            vectorList = [float(x) for x in vector.split()[2:]]
            query = self.create_query(vectorList,
                                      fts_raw_query_map, field_name, k_nearest_neighbour)
            r, a = self.query_and_calculate_recall_and_accuracy(query, indexName, truth)
            recall.append(r)
            accuracy.append(a)
        avg_recall = np.mean(recall)
        avg_accuracy = np.average(accuracy)
        logger.info(f"Recall distribution:\n {recall}")
        logger.info(f"Accuracy distribution:\n {accuracy}")
        return avg_recall, avg_accuracy
