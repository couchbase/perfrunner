import json

import requests

dataset = open("l_l.txt").read().splitlines()
result_dataset = list()


for line in dataset:
    words = line.split(' ')
    full_query = {}
    query = {"conjuncts": [{"field": "name", "term": words[0]},
                           {"field": "name", "term": words[1]}]}
    full_query["ctl"] = {"timeout": 0}
    full_query["size"] = 10
    full_query["query"] = query

    r = requests.post("http://172.23.99.211:8094/api/index/perf_fts_index/query",
                      data=json.dumps(full_query),
                      auth=('Administrator', 'password'))
    if json.loads(r.text)["total_hits"] > 10:
        result_dataset.append("{} {}".format(words[0], words[1]))
        if len(result_dataset) > 2000:
            break
        else:
            print((len(result_dataset)))

result_file = open("l_l_validated.txt", "w")
for line in result_dataset:
    print(line, file=result_file)
result_file.close()
