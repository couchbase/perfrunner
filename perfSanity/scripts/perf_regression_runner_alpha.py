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

def checkResults( results, testDescriptor):
            #print '\n\nthe results are', results
            p = re.compile(r'Dry run stats: {(.*?)}', re.MULTILINE)
            matches = p.findall(results.replace('\n', ''))
            results = []
            actual_values = {}
            for m in matches:
                #print '\n\nhave a match', m
                actual = json.loads('{' + m + '}')
                actual_values[actual['metric']] = actual  # ( json.loads('{' + m + '}') )

            expected_keys = testDescriptor['KPIs']
            for k in expected_keys.keys():
                haveAMatch = False
                for i in actual_values.keys():
                    if k in i:
                        haveAMatch = True
                        actualIndex = i
                        break
                        
                if haveAMatch:
                    passResult = True
                    if actual_values[actualIndex]['value'] > 1.1 * expected_keys[k]:
                        passResult = False
                        print '  ', k, ' is greater than expected. Expected', expected_keys[k], 'Actual', actual_values[actualIndex][
                            'value']

                    elif actual_values[actualIndex]['value'] < 0.9 * expected_keys[k]:
                        passResult = False
                        # sort of want to yellow flag this but for now all we have is a red flag so use that
                        print '  ', k, ' is less than expected. Expected', expected_keys[k], 'Actual', actual_values[actualIndex][
                            'value']

                    results.append({'testMetric':k, 'expectedValue':expected_keys[k], 'actualValue':actual_values[actualIndex]['value'], 'pass':passResult})
                    del actual_values[actualIndex]
                else:
                    print '  Expected key', k, ' is not found'

            if len(actual_values) > 0:
                print '  The following key(s) were present but not expected:'
                for i in actual_values:
                    print '    ', i
            return results



def runPerfRunner( testDescriptor, version, runStartTime, bucket ):
    print testDescriptor['testType']
    testName = testDescriptor['testName']

    testStartTime = time.strftime("%m/%d/%y-%H:%M:%S", time.strptime(time.ctime() ))
    startTime = time.time()   # in seconds to get the elapsed time
    print '\n\n', time.asctime( time.localtime(time.time()) ), 'Now running', testDescriptor


    test = testDescriptor['testFile'] + '.test'
    spec = 'perfSanity/clusters/' + testDescriptor['specFile'] + '.spec'
    KPIs = testDescriptor['KPIs']
    #current_summary = {'test': testDescriptor['testName'], 'status':'run', 'results':[]}

    my_env = os.environ
    my_env['cluster'] = spec
    my_env['test_config'] = 'perfSanity/tests/' + test
    my_env['version'] = version

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
        result =  {'runStartTime':runStartTime, 'testStartTime':testStartTime, 'build':version, 'testName':testName,
               'pass':False, 'reason':'Have an error during setup'}
        bucket.upsert( testStartTime + '-' + version + '-' + test, result,  format=couchbase.FMT_JSON)
        return False
    else:

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
            result =  {'runStartTime':runStartTime, 'testStartTime':testStartTime, 'build':version, 'testName':testName,
               'pass':False, 'reason':'Have an error during workload generation'}
            bucket.upsert( testStartTime + '-' + version + '-' + test, result,  format=couchbase.FMT_JSON)
            return False


        else:

            print '\n\nWorkload complete, analyzing results'

            results  = checkResults( workload_output, testDescriptor)
            commonData = {'runStartTime':runStartTime, 'build':version, 'testName':testName,
                            'testStartTime':testStartTime, 'elapsedTime': round(time.time() - startTime,0) }
            for i in results:
                bucket.upsert( testStartTime + '-' + version + '-' + i['testMetric'], dict(commonData.items() + i.items()), format=couchbase.FMT_JSON)




def runForestDBTest( testDescriptor, version, runStartTime, bucket  ):

    testStartTime = time.strftime("%m/%d/%y-%H:%M:%S", time.strptime(time.ctime() ))
    startTime = time.time()   # in seconds to get the elapsed time
    testName = testDescriptor['testName']

    command = testDescriptor['command'] + ' --version=' + version
    print 'the command is', command

    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,shell=True)
    commandOutput = ''
    for line in iter(proc.stdout.readline, ''):
        print line
        commandOutput += line

    (stdoutdata, stderrdata) = proc.communicate()

    print 'stderrdata', stderrdata

    if proc.returncode == 1:
        print '  Have an error during forest DB'
        result =  {'runStartTime':runStartTime, 'testStartTime':testStartTime, 'build':version,
                   'testName':testName,'pass':False, 'reason':'Check logs,'}
        bucket.upsert( testStartTime + '-' + version + '-' + testName, result, format=couchbase.FMT_JSON)
        return False
    else:
        results  = checkResults( commandOutput, testDescriptor)
        commonData = {'runStartTime':runStartTime, 'build':version, 'testName':testName,
                            'testStartTime':testStartTime, 'elapsedTime': round(time.time() - startTime,0) }
        for i in results:
            bucket.upsert( testStartTime + '-' + version + '-' + i['testMetric'], dict(commonData.items() + i.items()), format=couchbase.FMT_JSON)


def runTest( testDescriptor, version, runStartTime, bucket ):
    print testDescriptor['testType']
    testName = testDescriptor['testName']

    testStartTime = time.strftime("%m/%d/%y-%H:%M:%S", time.strptime(time.ctime() ))
    startTime = time.time()   # in seconds to get the elapsed time
    print '\n\n', time.asctime( time.localtime(time.time()) ), 'Now running', testName

    if testDescriptor['testType'] == 'perfRunner':
        runPerfRunner(testDescriptor, version, runStartTime, bucket)
    elif testDescriptor['testType'] == 'perfRunnerForestDB':
        print 'have the forest DB test', testDescriptor['command']
        runForestDBTest(testDescriptor, version, runStartTime, bucket)


def main():
    print 'Starting the perf regression runner'

    usage = '%prog -f conf-file'
    parser = OptionParser(usage)

    #parser.add_option('-f', '--filename', dest='filename')
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-r', '--runStartTime', dest='runStartTime')
    parser.add_option('-n', '--nop', dest='nop',default=False, action='store_true')

    options, args = parser.parse_args()

    runStartTime = options.runStartTime
    summary = []


    # open the bucket
    bucket = Bucket('couchbase://'+ '172.23.105.177:8091/Daily-Performance')

    testBucket = Bucket('couchbase://'+ '172.23.105.177:8091/Daily-Performance-Tests')
    queryString = "select `Daily-Performance-Tests`.* from `Daily-Performance-Tests`;"


    query = N1QLQuery(queryString )
    testsToRun = testBucket.n1ql_query( queryString )
    testsToRerun = []
    for row in testsToRun:
        if 'disabled' in row and row['disabled'].lower() == 'true':
            print row['testName'], ' is disabled.'
        else:
            if not runTest( row, options.version, options.runStartTime, bucket ):
                testsToRerun.append(row)

        #time.sleep(10)
    # end the for loop - print the results
    print 'done'
    return

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

