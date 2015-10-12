
import requests
import json
import re
import fileinput

from optparse import OptionParser




""" This was a previous iteration where we queried seriesly to get the data, but we don't do that anymore, but
    keeping this around in case it is needed in the future
"""


def verify_perf( host, db_name, perf_keys):
    session = requests.Session()
    url = 'http://{0}:{1}/'.format(host,3133)
    dbs = session.get(url + '_all_dbs').json()

    key_total_times = {}
    for i in perf_keys.keys():
        key_total_times[i] = {'sum':0.0, 'count':0}

    for i in dbs:
       if db_name in i:
           results = session.get(url + i + '/_all').json()

           #print 'results.values()', results.values()

           for sample in results.values():
              #import pdb;pdb.set_trace()
              for key, value in sample.items():
                  key_total_times[key]['sum'] += value
                  key_total_times[key]['count'] += 1
           for key, value in key_total_times.items():
               if value['count'] > 0:
                   avg_value = value['sum'] / value['count']
                   print key, 'has an average of', value['sum'] / value['count'], 'base on', value['count'], 'samples.'
                   if avg_value > 1.05 * perf_keys[key]:
                       print key,'value is greater than expected', perf_keys[key]

               else:
                   print key, ' had no samples'
       results = session.delete(url + i)


"""

# An evolving thing - takes as input:
- a file which is the output from perfrunner - this file will contain some json which describes the perf results
- the perf keys and expected values

This program parses out the results from the files and compares them against the expected values


"""

def main():

    usage = '%prog -d databasename -k tbd'
    parser = OptionParser(usage)

    #parser.add_option('-d', dest='dbname', help='database name')
    #parser.add_option('-s', dest='host')
    parser.add_option('-p', dest='perf_keys', help='keys to get and expected values - a dict')
    parser.add_option('-f','--filename', dest='filename')

    options, args = parser.parse_args()

    #print options.perf_keys

    #print 'perf keys are', json.loads(options.perf_keys)


    # read the input
    data = ''
    for line in fileinput.input(options.filename):
        data += line

    # parse the line
    p = re.compile(r'Dry run stats: {(.*?)}', re.MULTILINE)
    matches = p.findall(data.replace('\n',''))

    actual_values = {}
    for m in matches:
        actual = json.loads('{' + m + '}')
        actual_values[ actual['metric']] = actual    #( json.loads('{' + m + '}') )


    # and now verify what we saw with the expected

    have_an_error = False

    expected_keys = json.loads(options.perf_keys)
    for k in expected_keys.keys():
        if k in actual_values:
            if actual_values[k]['value'] > 1.05 * expected_keys[k]:
                print '\n', k, ' is greater than expected. Expected', expected_keys[k], 'Actual', actual_values[k]['value']
                have_an_error = True

            del actual_values[k]
        else:
            print '\nExpected key', k, ' is not found'
            have_an_error = True


    # check for unexpected keys
    if len(actual_values) > 0:
        print '\nThe following key(s) were present but not expected', actual_values
        have_an_error = True




if __name__ == "__main__":
    main()
