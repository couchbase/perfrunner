import json
import pprint

pp = pprint.PrettyPrinter(indent=4)


def main():
    doc_fields = ['name',
                  'email',
                  'alt_email',
                  'city',
                  'realm',
                  'coins',
                  'category',
                  'achievements',
                  'year',
                  'body']

    with open("collections/1bucket_1scope_100collections_basic.json", "r")\
            as read_file:
        index_map = json.load(read_file)

    for i in range(100):
        collection_indexes = {}
        num_coll_indexes = 0
        for field1 in doc_fields:
            for field2 in doc_fields:
                collection_indexes["myindex"+str(i+1)+"-"+str(num_coll_indexes+1)] = \
                    "{},{}".format(field1, field2)
                num_coll_indexes += 1
        index_map['bucket-1']['scope-1']['collection-'+str(i+1)] = collection_indexes
    outpath = 'tests/gsi/index_defs/'
    outfile = '1bucket_1scope_100collections_10k_indexes_1.json'
    with open(outpath+outfile, 'w') as outfile:
        json.dump(index_map, outfile, indent=4, sort_keys=True)
    pp.pprint(index_map)
    print("Done")


if __name__ == '__main__':
    main()
