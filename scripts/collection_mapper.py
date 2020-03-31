import json
import pprint
from argparse import ArgumentParser

pp = pprint.PrettyPrinter(indent=4)


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-b', '--buckets', dest='buckets',
                        required=True,
                        help='number of buckets')
    parser.add_argument('-s', '--scopes', dest='scopes',
                        required=True,
                        help='number of scopes')
    parser.add_argument('-c', '--collections', dest='collections',
                        required=True,
                        help='number of collections')
    parser.add_argument('-m', '--map_type', dest='map_type',
                        required=True,
                        help='collection map pattern')
    parser.add_argument('-l', '--load_pattern', dest='load_pattern',
                        required=False, default=None,
                        help='collection load pattern')
    parser.add_argument('-a', '--access_pattern', dest='access_pattern',
                        required=False, default=None,
                        help='collection access pattern')
    parser.add_argument('-p', '--print_json', dest='print_json',
                        required=False, default=False,
                        help='print collection map')

    return parser.parse_args()


def build_even(buckets, scopes, collections):
    temp_map = {}
    if buckets == 0:
        return temp_map
    if scopes == 0:
        scopes_per_bucket = 0
        collections_per_scopes = 0
    else:
        scopes_per_bucket = scopes//buckets
        collections_per_scopes = collections//scopes

    for i in range(1, buckets+1):
        bucket_name = "bucket-" + str(i)
        temp_map[bucket_name] = {}

    for bucket in temp_map:
        for i in range(1, scopes_per_bucket+1):
            scope_name = "scope-" + str(i)
            temp_map[bucket][scope_name] = {}

    collection_id = 1
    for bucket in temp_map:
        for scope in temp_map[bucket]:
            for i in range(1, collections_per_scopes+1):
                collection_name = "collection-" + str(collection_id)
                collection_id += 1
                temp_map[bucket][scope][collection_name] = {"load": 1, "access": 1}

    for bucket in temp_map:
        if len(temp_map[bucket].keys()) == 0:
            temp_map[bucket]["_default"] = {"_default": {"load": 1, "access": 1}}
        else:
            temp_map[bucket]["_default"] = {"_default": {"load": 0, "access": 0}}

    return temp_map


def main():
    args = get_args()

    map_type = args.map_type
    buckets = int(args.buckets)
    scopes = int(args.scopes)
    collections = int(args.collections)
    collection_map = {}

    if map_type == "even":

        # all buckets have same number of scopes
        # all scopes have same number of collections

        collection_map = build_even(buckets, scopes, collections)

    pp.pprint(collection_map)
    with open('1bucket_1scope_1000collections_basic.json', 'w') as outfile:
        json.dump(collection_map, outfile, indent=4, sort_keys=True)


if __name__ == '__main__':
    main()
