import requests
import json
import re
import fileinput

from optparse import OptionParser
import subprocess
import os
import sys
import time
from datetime import datetime
import pprint

from couchbase.bucket import Bucket
from couchbase.exceptions import *
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
flag= True

def create_cb_instance(server,bucket):
    try:
      url='couchbase://'+server+'/'+bucket
      return Bucket(url)
    except CouchbaseError as e:
        raise e

def load_cb_data(cb_instance,output,version,property,expected_result):

    data_to_load={}
    data_to_load['timestamp']=str(datetime.now())
    data_to_load['property']=property
    data_to_load['value']=output
    data_to_load['expected_value']=expected_result
    data_to_load['version']=version

    cb_instance.upsert(data_to_load['timestamp']+'_'+property+'_'+version ,data_to_load, format=couchbase.FMT_JSON)
    global flag
    if flag:
        #create index and execute only once
        flag = False
        #CREATE INDEX timestamp ON `bucket name`(timestamp);CREATE INDEX property ON `bucket name`(propert);

def cb_data_analysis(actual_values,test_name,iter,variation,expected_values):
      variation = float(variation)
      upper_variation = 1 + variation
      lower_variation = 1 - variation

      try:
          actual_values /= iter
      except ZeroDivisionError as e:
          raise e

      if actual_values > upper_variation * expected_values:
            print '  ', test_name, ' is greater than expected. Expected', expected_values, 'Actual', actual_values
            return False
      elif actual_values < lower_variation * expected_values:
            # sort of want to yellow flag this but for now all we have is a red flag so use that
            print '  ', test_name, ' is less than expected. Expected', expected_values, 'Actual', actual_values
            return False

      return True

def get_set_env(property,data,options):

    #ßproperty=property.upper()
    spec=  property["test_details"]["spec"]
    test=  property["test_details"]["test"]
    params=property["test_details"]["params"]
    test = 'perfSanity/tests/' + test
    spec = 'perfSanity/clusters/' + spec
    my_env = os.environ
    my_env['cluster'] = spec
    my_env['test_config'] = test
    my_env['version'] = options.version
    return my_env,test,spec,params

def main():
    print 'Starting the perf regression runner'

    usage = '%prog -f conf-file'
    parser = OptionParser(usage)

    parser.add_option('-f', '--filename', dest='filename')
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-s', '--summaryFile', dest='summaryFile')
    parser.add_option('-p','--property',dest='property')


    options, args = parser.parse_args()
    summary = []

    data=None
    try:
        with open(options.filename) as data_file:
            data = json.load(data_file)
    except (OSError, IOError,ValueError) as e:
        raise e

    archive_bucket = create_cb_instance(data["couchbase_server"],data["couchbase_bucket"])

    count = 0
    for property in options.property.split(','):
        for property_test in data["test_category"][property]:
            temp_data=[]
            my_env,test,spec,params=get_set_env(property_test,data)

            proc = subprocess.Popen('./scripts/setup.sh', env=my_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        shell=True)

            for line in iter(proc.stdout.readline, ''):
                print 'Setup output', line
                sys.stdout.flush()

            (stdoutdata, stderrdata) = proc.communicate()

            current_summary = {'test': test, 'status':'run', 'results':[]}
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
            flag = True
            while count < data["iteration"] and flag:

                print '\n\n', time.asctime( time.localtime(time.time()) ), 'Now running', test

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
                #workload_output = test_workload_output
                p = re.compile(r'Dry run stats: {(.*?)}', re.MULTILINE)
                matches = p.findall(workload_output.replace('\n', ''))

                actual_values = {}
                for m in matches:
                    actual = json.loads('{' + m + '}')
                    actual_values[actual['metric']] = actual
                print '\n\nWorkload gen output:', workload_output, '\n\n'

                expected_keys = params
                for k in expected_keys.keys():
                    pass
                    #load_cb_data(archive_bucket,900,options.version,property,data["test_category"][property]["test_details"]["actual_value"])
                    #current_summary['results'].append( {'metric':k, 'expected':expected_keys[k], 'actual':actual_values[k][ 'value']})
                    #temp_data.append(actual_values[k])
                summary.append(current_summary)
                print '\nCompleted analysis for', test
                time.sleep(10)
                #if cb_data_analysis(avg(temp_data),test,data["iteration"],data["variation"],property_test["test_details"]["actual_value"]):
                   #flag = False
                count += 1



if __name__ == "__main__":
    if not main():
        sys.exit(1)
