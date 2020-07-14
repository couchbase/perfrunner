import json
import pprint

pp = pprint.PrettyPrinter(indent=4)


def main():

    scan_map = {
        "Concurrency": 128,
        "Clients": 5,
        "ScanSpecs": [
        ]
    }

    with open("tests/gsi/index_defs/1bucket_1scope_1000collections_10k_indexes_1.json", "r")\
            as read_file:
        index_map = json.load(read_file)

    scan_specs = []
    bucket = 'bucket-1'
    scope = "scope-1"
    for i in range(1000):
        collection = "collection-{}".format(i+1)
        collection_index_defs = index_map[bucket][scope][collection]
        for index_name in collection_index_defs.keys():
            index_def = collection_index_defs[index_name]
            if index_def in ['name', 'email', 'alt_email', 'city',
                             'realm', 'country', 'county', 'street',
                             'body', 'street']:
                index_scan_def = {
                    "Limit": 1,
                    "Low": [
                        "000015"
                    ],
                    "Repeat": 19999,
                    "Type": "Range",
                    "Bucket": "bucket-1",
                    "Scope": scope,
                    "Collection": collection,
                    "High": [
                        "000028"
                    ],
                    "Id": 1,
                    "Inclusion": 3,
                    "Index": index_name,
                    "NInterval": 100
                }
            elif index_def == 'coins':
                index_scan_def = {
                    "Limit": 1,
                    "Low": [
                        0.21
                    ],
                    "Repeat": 19999,
                    "Type": "Range",
                    "Bucket": "bucket-1",
                    "Scope": scope,
                    "Collection": collection,
                    "High": [
                        0.4
                    ],
                    "Id": 1,
                    "Inclusion": 3,
                    "Index": index_name,
                    "NInterval": 100
                }
            elif index_def == 'category':
                index_scan_def = {
                    "Limit": 1,
                    "Low": [
                        1
                    ],
                    "Repeat": 19999,
                    "Type": "Range",
                    "Bucket": "bucket-1",
                    "Scope": scope,
                    "Collection": collection,
                    "High": [
                        2
                    ],
                    "Id": 1,
                    "Inclusion": 3,
                    "Index": index_name,
                    "NInterval": 100
                }
            elif index_def == 'year':
                index_scan_def = {
                    "Limit": 1,
                    "Low": [
                        1986
                    ],
                    "Repeat": 19999,
                    "Type": "Range",
                    "Bucket": "bucket-1",
                    "Scope": scope,
                    "Collection": collection,
                    "High": [
                        1987
                    ],
                    "Id": 1,
                    "Inclusion": 3,
                    "Index": index_name,
                    "NInterval": 100
                }
            else:
                raise Exception("bad")
            index_scan_def["Consistency"] = True
            scan_specs.append(index_scan_def)

    scan_map['ScanSpecs'] = scan_specs
    outpath = 'tests/gsi/moi/config/'
    outfile = 'config_scanlatency_sessionconsistent_10k_indexes_moi_1s_1000c_1.json'
    with open(outpath+outfile, 'w') as outfile:
        json.dump(scan_map, outfile, indent=4, sort_keys=True)
    pp.pprint(scan_map)
    print("Done")


if __name__ == '__main__':
    main()
