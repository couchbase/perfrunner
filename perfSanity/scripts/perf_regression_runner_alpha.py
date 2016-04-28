import requests
import json
import re
import fileinput

import argparse
import subprocess
import signal
from threading import Timer,Thread


import os
import sys
import time
import traceback
import string
import paramiko

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



# subprocess timeout code from here http://stackoverflow.com/questions/1191374/using-module-subprocess-with-timeout

def kill_proc(proc, timeout):
  timeout["value"] = True
  proc.kill()

def run_with_timeout(cmd, env, timeout_sec):
  print 'the timeout value is', timeout_sec
  proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  timeout = {"value": False}
  timer = Timer(timeout_sec, kill_proc, [proc, timeout])
  timer.start()

  # real time output is handy but it didn't seem to work with the timeout stuff
  #for line in iter(proc.stdout.readline, ''):
        #print 'workload line', line
        #workload_output += line



  print 'before communicate'
  stdout, stderr = proc.communicate()
  print 'after communicate'
  timer.cancel()
  return proc.returncode, stdout.decode("utf-8"), stderr.decode("utf-8"), timeout["value"]





def checkResults( results, testDescriptor, operatingSystem):
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
                largerIsBetter = None

                if type(expected_keys[k]) is int or type(expected_keys[k]) is float:
                     #print 'have old style kpi'
                     expected = expected_keys[k]
                elif type(expected_keys[k]) is dict:
                     #print 'have a new style dict'
                     if 'larger_is_better' in expected_keys[k]:
                         largerIsBetter = expected_keys[k]['larger_is_better']
                     if operatingSystem in expected_keys[k]:
                         expected = expected_keys[ k ] [operatingSystem] 
                     else:
                         print 'unsupported os', operatingSystem
                else:
                     print 'unexpected type', type(expected_keys[k])
                #print 'expected is', expected

                for i in actual_values.keys():
                    if k in i:
                        haveAMatch = True
                        actualIndex = i
                        break
                        
                if haveAMatch:
                    passResult = True
                    if largerIsBetter is True and actual_values[actualIndex]['value'] > expected:
                        print 'Larger is Better, so larger expected values are good!'
                        print '  ', k, ' is greater than expected. Expected', expected, 'Actual', actual_values[actualIndex][
                            'value']
                    elif largerIsBetter is False and actual_values[actualIndex]['value'] < expected:
                        print 'Larger is Better is False, so lower expected values are good!'
                        print '  ', k, ' is greater than expected. Expected', expected, 'Actual', actual_values[actualIndex][
                                                                                                  'value']
                    elif actual_values[actualIndex]['value'] > 1.1 * expected:
                        passResult = False
                        print '  ', k, ' is greater than expected. Expected', expected, 'Actual', actual_values[actualIndex][
                            'value']

                    elif actual_values[actualIndex]['value'] < 0.9 * expected:
                        passResult = False
                        # sort of want to yellow flag this but for now all we have is a red flag so use that
                        print '  ', k, ' is less than expected. Expected', expected, 'Actual', actual_values[actualIndex][
                            'value']

                    results.append({'testMetric':k, 'expectedValue':expected, 'actualValue':actual_values[actualIndex]['value'], 'pass':passResult})
                    del actual_values[actualIndex]
                else:
                    print '  Expected key', k, ' is not found'

            if len(actual_values) > 0:
                print '  The following key(s) were present but not expected:'
                for i in actual_values:
                    print '    ', i
            return results



platformDescriptor = {'windows':{'servers':['172.23.107.100','172.23.107.5','172.23.107.218'],'seriesly':'172.23.107.168','testClient':'172.23.107.168'}}

def executeRemoteCommand( cmd ):
       print 'executing command', cmd
       ssh = paramiko.SSHClient()
       ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
       ssh.connect('172.23.107.51', username='root', password='northscale!23')

       stdin, stdout, stderr = ssh.exec_command( cmd )

       for line in stdout.read().splitlines():
           print(line)
       #print 'stderr', stderr


revertUUIDs = ['fb4e923d-bff4-c3e0-6434-8657b8b5724a', 'fbddb566-51f4-0de5-f1a1-f6d33ef97793', '68752cfe-146d-7a2d-528c-b7f6b9a74eed' ]

VMids = ['s72606-w12r2-cbit-4683', 's72607-w12r2-cbit-4683', 's72608-w12r2-cbit-4683']

def resetWindowsServers():
    threads = []
    for s in revertUUIDs:
         cmd = 'xe snapshot-revert snapshot-uuid={0}'.format( s )
         t = Thread(target=executeRemoteCommand, args=(cmd,))
         t.start()
         t.join()
         threads.append( t )

    for t in threads:
        pass #t.join()

    time.sleep(120)
    threads = []
    for s in VMids:
         cmd = 'xe vm-start vm={0}'.format( s )
         t = Thread(target=executeRemoteCommand, args=(cmd,))
         t.start()
         t.join()
         threads.append( t )

    for t in threads:
        pass #t.join()


    # and then verify they are up
    RETRY_COUNT = 40
    for s in platformDescriptor['windows']['servers']:
        retries = 0
        print '\npinging', s,
        while retries < RETRY_COUNT:
            response = os.system("ping -c 1 -w2 " + s + " > /dev/null 2>&1")
            print 'response ', response,
            if response == 0:
                break
            else:
                print 'sleeping',
                time.sleep(15)
                retries = retries + 1

        if retries == RETRY_COUNT: return False

    return True

def updateSpecFile( fileName, os):

    f = open(fileName)
    data = f.readlines()
    f.close()

    for i in range( len(data) ):
          # really should have proper templates but for now use the CentOS values
          if '10.17.0.105' in data[i]:
             data[i] = string.replace(data[i], '10.17.0.105', platformDescriptor[os]['servers'][0] )
          elif '10.17.0.106' in data[i]:
             data[i] = string.replace(data[i], '10.17.0.106', platformDescriptor[os]['servers'][1] )
          elif '10.17.0.107' in data[i]:
             data[i] = string.replace(data[i], '10.17.0.107', platformDescriptor[os]['servers'][2] )
          elif '10.5.3.40' in data[i]:   # this is the test client host
             data[i] = string.replace(data[i], '10.5.3.40', platformDescriptor[os]['testClient'])

          if os == 'windows':
              # change the credentials
              if 'root:couchbase' in data[i] and 'credentials' not in data[i]:
                  data[i] = string.replace(data[i], 'root', 'Administrator')
                  data[i] = string.replace(data[i], 'couchbase', 'Membase123')

              # and the paths
              if '/opt/couchbase/var/lib/couchbase/data' in data[i]:
                  data[i] = string.replace(data[i], '/opt/couchbase/var/lib/couchbase/data', 'c:\data')
              if '/data/cbbackup_dir' in data[i]:
                  data[i] = string.replace(data[i], '/data/cbbackup_dir', 'c:\data')

    for d in data:
          print d,

    f = open(fileName, 'w')
    f.writelines(data)


def runPerfRunner( testDescriptor, options):
    print testDescriptor['testType']
    testName = testDescriptor['testName']



    testFile = 'perfSanity/tests/' + testDescriptor['testFile'] + '.test'
    if options.specFile is None:
        # backup and restore needs a list of spec files to do the installs
        if type(testDescriptor['specFile']) is unicode:
            spec = [ 'perfSanity/clusters/' + testDescriptor['specFile'] + '.spec' ]
        else:
            print 'have a list of spec files'
            spec = []
            for i in testDescriptor['specFile']:
                spec.append( 'perfSanity/clusters/' + i  + '.spec' )
    else:
        spec = ['perfSanity/clusters/' + options.specFile + '.spec']

    if options.os == 'windows':

        print 'start reset windows servers'
        res = resetWindowsServers()
        print 'done reset windows servers', res
        if not res:   # heck the Windows servers are not in good shape
            return  [{'pass':False, 'reason':'Failure during Windows snapshot revert'}]
        time.sleep(60)    # give some time for the system to settle down




    if options.os != 'centos':
         # change the .test file to point to the seriesly host
         cmd = "sed -i '/seriesly_host/c\seriesly_host = {0}' {1}".format(platformDescriptor[options.os]['seriesly'], testFile)
         print 'the command is', cmd
         proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

         # and mess with the memory quota
         cmd = "sed -i 's/mem_quota = 8048/mem_quota = 2048/' {0}".format( testFile )
         print 'the command is', cmd
         proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

         cmd = "sed -i 's/mem_quota = 5000/mem_quota = 2048/' {0}".format( testFile )
         print 'the command is', cmd
         proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

         # and update spec file with the ips
         for s in spec:
             updateSpecFile( s, options.os )


    KPIs = testDescriptor['KPIs']

    my_env = os.environ
    my_env['test_config'] = testFile
    if options.url is None:
        my_env['version'] = options.version
    else:
        my_env['url'] = options.url
    if 'override' in testDescriptor:
        print 'override is', testDescriptor['override']
        my_env['override'] = testDescriptor['override']
    else:
        my_env['override'] = ''


    for i in spec:
        my_env['cluster'] = i
        if False and options.query is None and re.search('n1ql.*Q[2].*',testName):
            print '-'*100
            print 'Skipping Setup for N1QL Q2 queries ... '
            print '-'*100
        else:
            proc = subprocess.Popen('./scripts/setup.sh', env=my_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

            for line in iter(proc.stdout.readline, ''):
                print 'Setup output', line,
                sys.stdout.flush()

            (stdoutdata, stderrdata) = proc.communicate()

            if proc.returncode == 1:
                print '\n\nHave an error during setup'
                print stderrdata
                print stdoutdata
                return  [{'pass':False, 'reason':'Have an error during setup'}]

            print 'Setup complete, starting workload'

    """ revert the hang detection - it did not detect the hang and suppressed the output
    rc, stdout, stderr, timeout = run_with_timeout( './perfSanity/scripts/workload_dev.sh', my_env, 7200)  # 2 hours
    print 'rc is', rc
    print 'timeout is', timeout
    print 'stderr', stderr
    print 'stdout is', stdout
    """

    # check for a looping process
    startTime = time.time()   # in seconds to get the elapsed time
    sys.stdout.flush()


    if options.patchScript is not None:
        print 'running patchScript', options.patchScript
        cmd = './perfSanity/scripts/' + options.patchScript
        print ' the command is ', cmd
        proc = subprocess.Popen(cmd, env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (stdoutdata, stderrdata) = proc.communicate()
        print stdoutdata
        if proc.returncode == 1:
                print '\n\nHave an error during patchScript'
                print stderrdata
                print stdoutdata
                return  [{'pass':False, 'reason':'Have an error during patchScript'}]



    proc = subprocess.Popen('./perfSanity/scripts/workload_dev.sh', env=my_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    workload_output = ''
    for line in iter(proc.stdout.readline, ''):
       if time.time() - startTime > 4000:   # 1 hour and a bit
            sys.stdout.flush()
            os.kill(proc.pid, signal.SIGUSR1)
            return  [{'pass':False, 'reason':'Command timed out'}]
       print line,
       workload_output += line

    (stdoutdata, stderrdata) = proc.communicate()

    print 'stderrdata', stderrdata



    if proc.returncode == 1:
        print '  Have an error during workload generation'
        return [{'pass':False, 'reason':'Have an error during workload generation'}]
    #elif timeout:
        #print '  test timeout'
        #return [{'pass':False, 'reason':'test timed out'}]
    else:
        print '\n\nWorkload complete, analyzing results'
        return checkResults( workload_output, testDescriptor, options.os)


def runForestDBTest( testDescriptor, options):

    if options.version == 'windows':
        return

    if options.url is not None:
        print 'runForestDBTest and url option is not supported'
        return []

    testName = testDescriptor['testName']

    command = testDescriptor['command'] + ' --version=' + options.version
    print 'the command is', command

    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,shell=True)
    commandOutput = ''
    for line in iter(proc.stdout.readline, ''):
        print line,
        commandOutput += line

    (stdoutdata, stderrdata) = proc.communicate()

    print 'stderrdata', stderrdata

    if proc.returncode == 1:
        print '  Have an error during forest DB'
        return [{'pass':False, 'reason':'Check logs'}]
    else:
        return checkResults( commandOutput, testDescriptor, options.os)


def runTest( testDescriptor, options, bucket, considerRerun ):
    print testDescriptor['testType']
    testName = testDescriptor['testName']

    testStartTime = time.strftime("%y-%m-%d-%H:%M:%S", time.strptime(time.ctime() ))
    startTime = time.time()   # in seconds to get the elapsed time
    print '\n\n', time.asctime( time.localtime(time.time()) ), 'Now running', testName

    baseResult = {'runStartTime':options.runStartTime, 'testStartTime':testStartTime, 'build':options.version, 'testName':testName,
                   'os':options.os}
    if testDescriptor['testType'] == 'perfRunner':
        res = runPerfRunner(testDescriptor, options)
    elif testDescriptor['testType'] == 'perfRunnerForestDB':
        print 'have the forest DB test', testDescriptor['command']
        res = runForestDBTest(testDescriptor, options)
    elif testDescriptor['testType'] == 'SampleDBs':
        return True
    else:
        print 'Unknown test type', testDescriptor['testType']
        return True


    # logic is a little complicated here. If this is the second time through we won't even consider the rerun, otherwise the criteria is
    #   1. There was a setup error, or,
    #   2. A passing test failed
    # Won't handle multiple failures
    rerun = False
    if considerRerun:
       if len(res) == 0 or 'reason' in res[0]: rerun = True         # something bad happened, must rerun

       elif testDescriptor['status'] == 'pass':    # check for failures in a passing test case
          for i in res:
             if 'pass' in i and not i['pass']:
                rerun = True

    for i in res:
        combinedResults = dict(baseResult.items() + i.items()  + {'elapsedTime': round(time.time() - startTime,0)}.items() )
        print 'the result is ', combinedResults
        if bucket is not None:
            if 'testMetric' in combinedResults:
                testKey = combinedResults['testName'] + '-' + combinedResults['testMetric']
            else:
                testKey = combinedResults['testName']
            bucket.upsert( testStartTime + '-' + options.version + '-' + testKey, combinedResults, format=couchbase.FMT_JSON)

    return not rerun

def main():
    print 'Starting the perf regression runner'

    usage = '%prog -f conf-file'
    #parser = OptionParser(usage)

    ##parser.add_option('-f', '--filename', dest='filename')
    #parser.add_option('-v', '--version', dest='version')
    #parser.add_option('-u', '--url', dest='url')
    #parser.add_option('-q', '--query', dest='query')
    #parser.add_option('-s', '--specFile', dest='specFile')
    #parser.add_option('-r', '--runStartTime', dest='runStartTime')
    #parser.add_option('-b', '--betaTests', dest='betaTests', default=False, action='store_true')
    #parser.add_option('-n', '--nop', dest='nop',default=False, action='store_true')

    #options, args = parser.parse_args()

    # the option parsing way
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument('-q', '--query', nargs='+')
    parser.add_argument('-v', '--version', dest='version')
    parser.add_argument('-u', '--url', dest='url')
    parser.add_argument('-s', '--specFile', dest='specFile')
    parser.add_argument('-r', '--runStartTime', dest='runStartTime')
    parser.add_argument('-b', '--betaTests', dest='betaTests', default=False, action='store_true')
    parser.add_argument('-a', '--allTests', dest='allTests', default=False, action='store_true')
    parser.add_argument('-n', '--nop', dest='nop',default=False, action='store_true')
    parser.add_argument('-p', '--patchScript', dest='patchScript',default=None)
    parser.add_argument('-o', '--os', dest='os',default='centos')
    parser.add_argument('-e', '--rerun', dest='rerun',default=True, action='store_false')
    parser.add_argument('-y', '--queryOnly', dest='queryOnly',default=False, action='store_true')

    options = parser.parse_args()



    print 'query', options.query


    print 'specfile', options.specFile


    print 'the os is', options.os
    runStartTime = options.runStartTime
    summary = []


    print 'rerun', options.rerun
    print 'url', options.url
    releaseVersion = float( '.'.join( options.version.split('.')[:2]) )
    print 'the release version is', releaseVersion

    # open the bucket
    if options.nop:
        bucket = None
    else:
        bucket = Bucket('couchbase://'+ '172.23.105.177:8091/Daily-Performance')

    testBucket = Bucket('couchbase://'+ '172.23.105.177:8091/Daily-Performance-Tests')
    queryString = "select `Daily-Performance-Tests`.* from `Daily-Performance-Tests` where status != 'unimplemented'"
    wherePredicates = []
    if options.query is not None:
        wherePredicates.append( ' '.join(options.query) )
    if not options.allTests:
        if options.betaTests:
            wherePredicates.append( "status='beta'")
        else:
            wherePredicates.append( "status!='beta'")

    if len(wherePredicates) > 0:
       for i in range(len(wherePredicates)):
            queryString += ' and ' + wherePredicates[i]

    # check for versioning
    queryString += ' and (implementedIn is missing or {0} >= implementedIn)'.format( releaseVersion)

    print 'the query string is', queryString
    query = N1QLQuery(queryString )
    testsToRun = testBucket.n1ql_query( queryString )
    tests = [row for row in testsToRun]
    print 'the tests are', len(tests), tests
    testsToRerun = []

    if options.queryOnly:
        return


    for row in tests:
        try:
            if row['status'].lower() == 'disabled':
                print row['testName'], ' is disabled.'
            else:
                if not runTest( row, options, bucket, considerRerun=options.rerun ):
                    testsToRerun.append(row)
        except:
            print 'Exception in ', row['testName']
            traceback.print_exc()

    # end the for loop - print the results
    print 'tests to rerun are', len(testsToRerun), testsToRerun
    if options.rerun:
        for i in testsToRerun: runTest( i, options, bucket, considerRerun=False )
 


if __name__ == "__main__":
    if not main():
        sys.exit(1)

