from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.exceptions import *
import couchbase
from datetime import datetime


class manage_test_result(object):

    def __init__(self):
        self.__couchbase_instance = {}
        self.__test_id=None
        self.__query_log = []

    def create_query_log(self,query_used,result):
        result_load={}
        result['query_used'] = query_used
        result['result_log'] = result
        self.__query_log.append(result_load)

    def load_data_query_benchmark(self,bucket,test,test_id,version):
        cb_instance = self.__couchbase_instance[bucket]
        data_to_load={}
        data_to_load['test_name']=test
        data_to_load['query_log']= self.__query_log
        data_to_load['jenkins_ID']=test_id
        data_to_load['version'] = version
        data_to_load['timestamp']=str(datetime.now())
        cb_instance.upsert(str(datetime.now())+'query',data_to_load,format=couchbase.FMT_JSON)

    def load_cb_data_sanity(self,bucket,output,version,property,expected_result,analysis,test_id,metric,test):

        cb_instance = self.__couchbase_instance[bucket]
        data_to_load={}
        data_to_load['timestamp']=str(datetime.now())
        data_to_load['property']=property
        data_to_load['result_value']=output
        data_to_load['expected_value']=expected_result
        data_to_load['version']=version
        data_to_load['iteration_run']=len(output)
        data_to_load["analysis_log"]=analysis
        data_to_load['jenkins_ID']=test_id
        data_to_load['tests_metric'] = metric
        data_to_load['test_name']=test

        cb_instance.upsert(data_to_load['timestamp']+'_'+property+'_'+version ,data_to_load, format=couchbase.FMT_JSON)

        print 'data load is done'


    def cb_load_test(self,bucket,data):
        '''
            It loads the test data in the couchbase
            :param cb_instance:
            :param data:
            :return:nothing
        '''
        cb_instance = self.__couchbase_instance[bucket]
        try:
          for property in data["test_category"]:
              primary_key= 'test_'+property
              value=data["test_category"][property]
              cb_instance.upsert(primary_key,value)

        except CouchbaseError :
            pass


    def create_report_sanity(self,bucket,jenkins_id):
        cb_instance = self.__couchbase_instance[bucket]
        query=N1QLQuery('SELECT * FROM `QE-Performance-Sanity`  where jenkins_ID= $test_id',test_id=jenkins_id)
        for rows in cb_instance.n1ql_query(query):
            result_value =  rows['QE-Performance-Sanity']['result_value']
            test_name = rows['QE-Performance-Sanity']['test_name']
            analysis = rows['QE-Performance-Sanity']['analysis_log']
            test_type = rows['QE-Performance-Sanity']['property']

            result_str =  'test:=> ' + test_name +  ' test_type:=> ' + test_type +  ' results obtained:=> ' + str(result_value[-1]) + ' analysis:=> ' +str(analysis[-1]) +'\n'
            print result_str


    def create_cb_instance(self,server,bucket):
        try:
          url='couchbase://'+server+'/'+bucket
          self.__couchbase_instance[bucket]=Bucket(url)
        except CouchbaseError as e:
            raise e

    def set_test_id(self,test_id):
        self.__test_id =  test_id

    def get_test_id(self):
        return self.__test_id
