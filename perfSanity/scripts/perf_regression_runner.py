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


"""

# An evolving thing - takes as input:
- a file which is the output from perfrunner - this file will contain some json which describes the perf results
- the perf keys and expected values

This program parses out the results from the files and compares them against the expected values


"""


test_workload_output = '''
[20/Oct/2015 15:01:26] INFO - Creating new database: iostatperfregression_410-4859-enterprise_27b10170106
[20/Oct/2015 15:01:29] INFO - Creating new database: ns_serverperfregression_410-4859-enterprise_27bbucket-110170107
[20/Oct/2015 15:02:08] INFO - Adding snapshot: perfregression_410-4859-enterprise_27b_access
[20/Oct/2015 15:02:15] INFO - http://cbmonitor.sc.couchbase.com/reports/html/?snapshot=perfregression_410-4859-enterprise_27b_access
[20/Oct/2015 15:03:04] INFO - http://cbmonitor.sc.couchbase.com/reports/get_corr_matrix/?snapshot=perfregression_410-4859-enterprise_27b_access
[20/Oct/2015 15:03:31] INFO - Dry run stats: {
    "build": "4.1.0-4859-enterprise", 
    "build_url": null, 
    "metric": "perf_sanity_kv_latency_mixed_2M_short_get_95th_perf_sanity_base_test", 
    "snapshots": [
        "perfregression_410-4859-enterprise_27b_access"
    ], 
    "value": 0.56
}
[20/Oct/2015 15:03:31] INFO - Dry run stats: {
    "build": "4.1.0-4859-enterprise", 
    "build_url": null, 
    "metric": "perf_sanity_kv_latency_mixed_2M_short_set_95th_perf_sanity_base_test", 
    "snapshots": [
        "perfregression_410-4859-enterprise_27b_access"
    ], 
    "value": 0.95
}
[20/Oct/2015 15:03:31] INFO - Terminating local Celery workers
'''

def main():
    print 'Starting the perf regression runner'

    usage = '%prog -f conf-file'
    parser = OptionParser(usage)

    parser.add_option('-f', '--filename', dest='filename')
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-r', '--runStartTime', dest='runStartTime')

    options, args = parser.parse_args()
    summary = []


    # open the bucket
    bucket = Bucket('couchbase://'+ '172.23.105.177:8091/Daily-Performance')


    runStartTime = options.runStartTime #time.strftime("%m/%d/%y-%H:%M:%S", time.strptime(time.ctime() ))

    print 'run start time is', runStartTime

    for line in fileinput.input(options.filename):

        time.sleep(10)
        testStartTime = time.strftime("%m/%d/%y-%H:%M:%S", time.strptime(time.ctime() ))

        if line[0] == '#' or len(line.strip()) == 0:
            continue

        test, spec, params = line.split()
        print '\n\n', time.asctime( time.localtime(time.time()) ), 'Now running', test
        current_summary = {'test': test, 'status':'run', 'results':[]}

        spec = 'perfSanity/clusters/' + spec

        my_env = os.environ
        my_env['cluster'] = spec
        my_env['test_config'] = 'perfSanity/tests/' + test
        my_env['version'] = options.version
        # proc = subprocess.Popen('ls', env=my_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        #"""
        proc = subprocess.Popen('./scripts/setup.sh', env=my_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                shell=True)

        for line in iter(proc.stdout.readline, ''):
            print 'Setup output', line
            sys.stdout.flush()

        (stdoutdata, stderrdata) = proc.communicate()

        if proc.returncode == 1:
            print '\n\nHave an error during setup'
            print stderrdata
            print stdoutdata
            current_summary['output'] = '  Have an error during setup'
            current_summary['status'] = 'not run'
            summary.append(current_summary)
            continue

        print 'Setup complete, starting workload'
        sys.stdout.flush()
        proc = subprocess.Popen('./perfSanity/scripts/workload_dev.sh', env=my_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        workload_output = ''
        for line in iter(proc.stdout.readline, ''):
            print line
            workload_output += line

        (stdoutdata, stderrdata) = proc.communicate()

        print 'stderrdata', stderrdata

        if proc.returncode == 1:
            print '  Have an error during workload generation'
            current_summary['output'] = '  Have an error during workload generation'
            sys.stdout.flush()
            current_summary['status'] = 'not run'
            print stderrdata
            summary.append(current_summary)
            continue

        print '\n\nWorkload complete, analyzing results'
        #"""
        # parse the line for actual values
        #workload_output = test_workload_output
        p = re.compile(r'Dry run stats: {(.*?)}', re.MULTILINE)
        matches = p.findall(workload_output.replace('\n', ''))

        actual_values = {}
        for m in matches:
            actual = json.loads('{' + m + '}')
            actual_values[actual['metric']] = actual  # ( json.loads('{' + m + '}') )
        print '\n\nWorkload gen output:', workload_output, '\n\n'

        expected_keys = json.loads(params)
        for k in expected_keys.keys():
            if k in actual_values:
                passResult = True
                current_summary['results'].append( {'metric':k, 'expected':expected_keys[k], 'actual':actual_values[k][ 'value']})
                if actual_values[k]['value'] > 1.1 * expected_keys[k]:
                    passResult = False
                    print '  ', k, ' is greater than expected. Expected', expected_keys[k], 'Actual', actual_values[k][
                        'value']

                elif actual_values[k]['value'] < 0.9 * expected_keys[k]:
                    passResult = False
                    # sort of want to yellow flag this but for now all we have is a red flag so use that
                    print '  ', k, ' is less than expected. Expected', expected_keys[k], 'Actual', actual_values[k][
                        'value']

    		data = {'runStartTime':runStartTime, 'build':options.version, 'testName':test, 
                       'testMetric':k, 'expectedValue':expected_keys[k], 'actualValue':actual_values[k]['value'], 'pass':passResult, 
			'testStartTime':testStartTime}
                bucket.upsert( runStartTime + '-' + options.version + '-' + test + '-' + k, data,  format=couchbase.FMT_JSON)

                del actual_values[k]
            else:
                current_summary['results'].append( {'metric':k, 'expected':'Not found' , 'actual':k})
                print '  Expected key', k, ' is not found'
                #current_summary['output'] += '\n    Expected key {0} is not found'.format(k)

        if len(actual_values) > 0:
            print '  The following key(s) were present but not expected:'
            for i in actual_values:
                print '    ', i
                current_summary['results'].append( {'metric':k, 'expected':i , 'actual':'Not found'})

        summary.append(current_summary)
        print '\nCompleted analysis for', test
        time.sleep(10)
    # end the for loop - print the results

    all_success = True

    print '\n\nTest\t\t\t\t\t\t\tMetric\t\t\t\t\t\t\tActual\t\t\tExpected'
    for i in summary:
        if i['status'] == 'not run':
           print i['test'], 'not run:', i['output']
           all_success = False
        else:
            for j in i['results']:
               print i['test'], '\t', j['metric'], '\t', j['actual'], '\t', j['expected'],
               if j['actual'] == 'Not found' or j['expected'] == 'Not found':
                  all_success = False
               else:
                  # do the compares
                  if j['actual']  > 1.1 * j['expected']:
                      print '\tactual is too large',
                      all_success = False
                  elif j['actual']  < 0.9 * j['expected']:
                      print '\tactual is too small',
                      all_success = False
               print
        print # blank line at end of test

    #endfor all tests
    return all_success


if __name__ == "__main__":
    if not main():
        sys.exit(1)
