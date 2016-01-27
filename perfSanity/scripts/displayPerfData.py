import requests
import json
import re
import fileinput

from optparse import OptionParser
import subprocess
import os
import sys
import time


from couchbase.bucket import Bucket
import couchbase
from couchbase.n1ql import N1QLQuery


"""

# An evolving thing - takes as input:
- the time that it was run


"""


# taken from here https://www.calazan.com/python-function-for-displaying-a-list-of-dictionaries-in-table-format/
def format_as_table(data,
                    keys,
                    header=None,
                    sort_by_key=None,
                    sort_order_reverse=False):
    """Takes a list of dictionaries, formats the data, and returns
    the formatted data as a text table.

    Required Parameters:
        data - Data to process (list of dictionaries). (Type: List)
        keys - List of keys in the dictionary. (Type: List)

    Optional Parameters:
        header - The table header. (Type: List)
        sort_by_key - The key to sort by. (Type: String)
        sort_order_reverse - Default sort order is ascending, if
            True sort order will change to descending. (Type: Boolean)
    """
    # Sort the data if a sort key is specified (default sort order
    # is ascending)
    if sort_by_key:
        data = sorted(data,
                      key=itemgetter(sort_by_key),
                      reverse=sort_order_reverse)

    # If header is not empty, add header to data
    if header:
        # Get the length of each header and create a divider based
        # on that length
        header_divider = []
        for name in header:
            header_divider.append('-' * len(name))

        # Create a list of dictionary from the keys and the header and
        # insert it at the beginning of the list. Do the same for the
        # divider and insert below the header.
        header_divider = dict(zip(keys, header_divider))
        data.insert(0, header_divider)
        header = dict(zip(keys, header))
        data.insert(0, header)

    column_widths = []
    for key in keys:
        column_widths.append(max(len(str(column[key])) for column in data))

    # Create a tuple pair of key and the associated column width for it
    key_width_pair = zip(keys, column_widths)

    format = ('%-*s ' * len(keys)).strip() + '\n'
    formatted_data = ''
    for element in data:
        data_to_format = []
        # Create a tuple that will be used for the formatting in
        # width, value format
        for pair in key_width_pair:
            data_to_format.append(pair[1])
            data_to_format.append(element[pair[0]])
        formatted_data += format % tuple(data_to_format)
    return formatted_data


def main():
    print 'Starting the perf regression runner'

    usage = '%prog -f conf-file'
    parser = OptionParser(usage)

    parser.add_option('-r', '--runStart', dest='runStart')
    parser.add_option('-v', '--version', dest='version')

    options, args = parser.parse_args()
    summary = []


    # open the bucket
    bucket = Bucket('couchbase://172.23.105.177/Daily-Performance')


    print 'the run id is', options.runStart


    # query for everything based on the run id
    queryBaseString = """
    select testName, testMetric, pass, expectedValue,actualValue,`build` from `Daily-Performance`
    where runStartTime = '{0}' and `build`='{1}';
    """

    queryString = queryBaseString.format(options.runStart, options.version)


    print 'the query is', queryString #.format(options.run, componentString)
    query = N1QLQuery(queryString )
    results = bucket.n1ql_query( queryString )


    resultList = []
    for row in results:
        #print 'row is ',row
        # a bit of a hack to remove the redundant information
        row['testName'] = row['testName'].replace('perf_sanity_','')   #perf_sanity_....   .test
        row['testMetric'] = row['testMetric'].replace('perf_sanity_','').replace('_base_test','') #.replace('_perf_sanity_secondary','')
        resultList.append(row)

    #import pdb;pdb.set_trace()

    print; print
    print format_as_table( resultList, ['testName','testMetric','pass','expectedValue','actualValue'],
                           ['Test Name','Metric','Pass?','Expected','Actual'] )



if __name__ == "__main__":
    main()
