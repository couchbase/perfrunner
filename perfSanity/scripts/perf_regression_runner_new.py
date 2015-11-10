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


from  perf_data_management import manage_test_result

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

def cb_data_analysis(actual_values,test_name,variation,params,analysis_data):

      variation = .05 #float(variation)
      upper_variation = 1 + variation
      lower_variation = 1 - variation
      print '\n analysis of result \n'
      result = True

      temp_analysis_data=[]
      for k in params.keys():
           if actual_values[k]['value'] > upper_variation * params[k]:
                print '  ', test_name, ' is greater than expected. Expected value for key ',k, ' ', params[k], ' Actual ', actual_values[k]['value'],'\n'
                temp_analysis_data.append(test_name+' expected result :=> '+str(params[k])+' :Result greater than expected , test result:=> '+ str(actual_values[k]['value']))
                result *=False
           elif actual_values[k]['value'] < lower_variation * params[k]:
                # sort of want to yellow flag this but for now all we have is a red flag so use that
                print '  ', test_name, ' is less than expected. Expected for key ',k, ' ', params[k], 'Actual ', actual_values[k]['value'],'\n'

                temp_analysis_data.append(test_name+' expected result :=> '+str(params[k])+' :Result less than expected , test result:=> '+ str(actual_values[k]['value']))
                result *= False
           else:
             result *= True
             print test_name ,'  ' ,params[k],  ' result is expected'
             temp_analysis_data.append(test_name+' expected result :=> '+str(params[k])+' :Result is expected value :=> ' + str(actual_values[k]['value']))
      analysis_data.append(temp_analysis_data)

      return result


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

def get_set_env(property,options):

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
    parser.add_option('-b','--build',dest='build')
    parser.add_option('-t','--tag',dest='tag')


    options, args = parser.parse_args()
    summary = []

    mng_data = manage_test_result()
    data=None
    try:
        with open(options.filename) as data_file:
            data = json.load(data_file)
    except (OSError, IOError,ValueError) as e:
        raise e

    mng_data.create_cb_instance(data["couchbase_server"],data["couchbase_bucket"])
    mng_data.create_cb_instance(data["couchbase_server"],data["couchbase_test_bucket"])

    test_id=options.tag+ '_'+options.build
    mng_data.cb_load_test(data["couchbase_test_bucket"],data)
    mng_data.set_test_id(test_id)
    count = 0
    for property in options.property.split(','):
        for property_test in data["test_category"][property]:
            temp_data=[]
            analysis_data=[]
            my_env,test,spec,params=get_set_env(property_test,options)

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
            count = 0
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
                print "------- actual values ----------\n"
                print actual_values
                tmp=[]
                for k in expected_keys.keys():
                   tmp.append(actual_values[k]['value'])
                temp_data.append(tmp)
                print '\nCompleted analysis for', test
                time.sleep(10)
                if cb_data_analysis(actual_values,test,data["variation"],params,analysis_data):
                   flag = False
                summary.append(current_summary)
                print '\nCompleted analysis for', test
                time.sleep(10)
                count += 1
            iter_str='test result of : ' + str(len(analysis_data)) + ' iteration'
            analysis_data.insert(0,iter_str)
            mng_data.load_cb_data_sanity(data["couchbase_bucket"],temp_data,options.version,property,expected_test_result,analysis_data,mng_data.get_test_id(),params.keys(),test)

    mng_data.create_report_sanity(data["couchbase_bucket"],mng_data.get_test_id())


if __name__ == "__main__":
    if not main():
        sys.exit(1)
