import requests
import json
import re
import fileinput

from optparse import OptionParser
import subprocess
import os
import sys
import time
from copy import deepcopy


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
            header_divider.append('-' * len(name.rstrip()))

        # Create a list of dictionary from the keys and the header and
        # insert it at the beginning of the list. Do the same for the
        # divider and insert below the header.
        header_divider = dict(zip(keys, header_divider))
        data.insert(0, header_divider)
        header = dict(zip(keys, header))
        data.insert(0, header)

    column_widths = []
    for key in keys:
        for column in data:
            if key not in column:
                print 'bad column', key, column
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


def munge_test_name_and_metric( name, metric):
    name = name.replace('perf_sanity_','')

    metric = metric.replace('perf_sanity_','').replace('_base_test','') #.replace('_perf_sanity_secondary','')
    if metric == 'avg_query_requests': metric = 'throughput'
    if 'n1ql_thr_lat_' in metric : metric= 'latency'
    return name, metric

def main():

    usage = '%prog -f conf-file'
    parser = OptionParser(usage)

    parser.add_option('-r', '--runStart', dest='runStart')
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-p', '--historicalData', dest='historicalData', default=False, action='store_true')

    options, args = parser.parse_args()
    summary = []


    # open the bucket
    resultsBucket = Bucket('couchbase://172.23.105.177/Daily-Performance')
    testDescriptorBucket = Bucket('couchbase://172.23.105.177/Daily-Performance-Tests')




    # query for everything based on the run id
    queryBaseString = """
    select testName, testMetric, pass, expectedValue,actualValue,`build`, reason,elapsedTime from `Daily-Performance`
    where runStartTime = '{0}' and `build`='{1}'  order by testName, testMetric, pass;
    """


    if options.historicalData:
        # get the historical results, for now this is 4.1.1
        historicalResultDescriptor = [{'release':'4.1.1','build':'5914','runStartTime':'2016-05-21:06:50'},
                                      {'release':'4.1.2','build':'6036','runStartTime':'2016-05-30:10:14'},
                                      {'release':'4.7.0','build':'711','runStartTime':'2016-05-27:17:40'},
                                    ]

        historicalReleases = [i['release'] for i in historicalResultDescriptor]
        print 'historicalReleases', historicalReleases
        historicalResults = {}
        for h in historicalResultDescriptor:
            print h
            historicalResults[h['release']] = {}
            queryString = queryBaseString.format(h['runStartTime'], '{0}-{1}'.format(h['release'], h['build']))
            query = N1QLQuery(queryString )
            print 'query:', queryString
            results = resultsBucket.n1ql_query( queryString )
            for row in results:
                if 'testName' in row and 'testMetric' in row and 'actualValue' in row:
                    row['testName'], row['testMetric'] = munge_test_name_and_metric(row['testName'], row['testMetric'])
                    historicalResults[h['release']] [ row['testName']+'-'+row['testMetric']] = row['actualValue']
                #print row

        #print 'historical results', historicalResults



    queryString = queryBaseString.format(options.runStart, options.version)
    #print 'the query is', queryString #.format(options.run, componentString)
    query = N1QLQuery(queryString )
    results = resultsBucket.n1ql_query( queryString )



    passingTests = []
    failingTests = []
    stabilizingTests = []
    environmentalIssues = []
    passedOnSecondRun  = []
    failedOnSecondRun = []



    # just get the good results, in the case where we run twice, also join with other data (I think)
    for row in results:
        #print 'row is ',row
        # a bit of a hack to remove the redundant information
        #print 'the row is', row


        #row['testName'] = row['testName'].replace('perf_sanity_','')   #perf_sanity_....   .test

        if 'testMetric' in row:

            row['testName'], row['testMetric'] = munge_test_name_and_metric(row['testName'], row['testMetric'])

            # munge the names
            #row['testMetric'] = row['testMetric'].replace('perf_sanity_','').replace('_base_test','') #.replace('_perf_sanity_secondary','')
            #if row['testMetric'] == 'avg_query_requests': row['testMetric'] = 'throughput'
            #if 'n1ql_thr_lat_' in row['testMetric'] : row['testMetric'] = 'latency'


            # check for other stuff like MBs
            row['jira'] = ''
            row['notes'] = ''
            try:
                res = testDescriptorBucket.get(row['testName']).value
                #print '****res is', res
                if 'notes' in res: row['notes'] = res['notes']
                if 'jira' in res: row['jira'] = res['jira']
                if 'status' in res: row['status'] = res['status']
                if 'elapsedTime' not in row:
                    row['elapsedTime'] = ''
                else:
                    row['elapsedTime'] = row['elapsedTime'] /60

                if options.historicalData:
                    for h in historicalReleases:
                        #print 'h is', historicalResults[h]
                        metric = row['testName']+'-'+row['testMetric']
                        if metric in historicalResults[h]:
                            row[h] = historicalResults[h][metric]
                        else:
                            row[h] = ''
                        #print 'h', h, 'row[h]', row[h]
            except:
                print 'no record for',row['testName']
                continue
            #print row

            if row['status'] == 'beta' or row['notes'] == 'stabilizing':
                stabilizingTests.append( row)
            else:
                # check if this test failed already
                alreadyPassed = False
                alreadyFailed = False
                for i in failingTests:
                    if row['testMetric'] == i['testMetric'] and row['testName'] == i['testName']:
                        alreadyFailed =True
                        break

                for i in passingTests:
                    if row['testMetric'] == i['testMetric'] and row['testName'] == i['testName']:
                        alreadyPassed =True
                        break

                if row['pass']:
                    if alreadyPassed:
                        pass # passed twice, this is a no-op
                    else:
                        passingTests.append( row )
                        if alreadyFailed:
                            # remove the failing record
                            testToRemove = -1
                            for i in range(len(failingTests)):
                                if row['testName'] == failingTests[i]['testName'] and \
                                    row['testMetric'] == failingTests[i]['testMetric']:
                                    testToRemove = i
                                    passedOnSecondRun.append(failingTests[i])
                                    break
                            if testToRemove >= 0:
                                failingTests.pop(testToRemove)

                else:   # failed this time
                    if alreadyFailed:
                        pass # if failed twice already have it
                    elif alreadyPassed:
                        failedOnSecondRun.append( row ) # and keep the pass
                    else:
                        failingTests.append( row )

        else:
            environmentalIssues.append( row )


    filteredEnvironmentalIssues = []
    for e in environmentalIssues:
        haveAnotherResult = False
        for p in passingTests:
            if e['testName'] == p['testName']:
                #environmentalIssues.remove(e)
                haveAnotherResult = True
                break
        for p in failingTests:
            if e['testName'] == p['testName']:
                #environmentalIssues.remove(e)
                haveAnotherResult = True
                break
        if not haveAnotherResult:
            filteredEnvironmentalIssues.append(e)

    summary = 'Performance Daily Sanity {0}: {1} total tests, {2} pass, {3} failures'.format( options.version,
                 len(passingTests) + len(failingTests) + len(filteredEnvironmentalIssues),  len(passingTests),
                    len(failingTests) )
    #if len(environmentalIssues) > 0:
        #summary = '{0} and {1} tests without results due to enviromental issues'.format( summary, len(filteredEnvironmentalIssues) )
    print summary



    # now do stuff by component

    allResults = passingTests + failingTests + filteredEnvironmentalIssues

    allResults = sorted(allResults, key=lambda k: k['testName'])



    # get all the tests
    query = N1QLQuery('select `Daily-Performance-Tests`.* from `Daily-Performance-Tests`' )
    allTests = [r for r in resultsBucket.n1ql_query( query ) if 'testName' in r]

    #print 'all the tests are',results



    skeletonRecord = {'testMetric':'', 'expectedValue':'','actualValue':'','elapsedTime':'','notes':''}
    smallerSkeletonRecord = {'expectedValue':'','actualValue':'','elapsedTime':''}

    blankRecord = {'testName':'','testMetric':'','expectedValue':'','actualValue':'','elapsedTime':'','notes':''}
    components = ['forest','dcp', 'kv', 'reb','view','xdcr', 'restore', 'secondary','n1ql','fts','beer','united','sabre']
    componentNames = { 'forest':'Forest DB','reb':'Rebalance', 'restore':'Restore',
                     'view':'Views','secondary':'2i','beer':'Beer queries', 'united':'United Queries',
                     'sabre':'Sabre Queries'}
    componentSummary = {}
    for i in components:
        componentSummary[i] = {'component':componentNames[i] if i in componentNames else i.upper(),
                               'passCount':0,'failCount':0,'elapsedTime':0}

    #components = ['united','sabre']
    allTestResults = []
    # append to the output by component
    for c in components:
        testsWithResults = []
        testsWithoutResults = []
        componentName = componentNames[c] if c in componentNames else c.upper()
        allTestResults.append( dict( {'testName':componentName,'testMetric':'','isAHeader':True}.items() + skeletonRecord.items()) )

        # allTest is all tests rom the DB
        for test in allTests:
            # is the test in the component?
            if c in test['testName'].lower():
                if 'status' in test and  test['status'] in  ['beta','disabled','unimplemented'] and test['status'] not in test['testName']:
                    #print 'have a beta etc',result
                     test['testMetric'] =  test['status']

                notes = test['jira'] if 'jira' in test else ''
                notes += test['notes'] if 'notes' in test else ''


                haveAtLeastOneResult = False
                # this test is part of component, check for results for this test
                for result in allResults:
                    if result['testName'] == test['testName']:
                        #print 'have a result', c,  test, result
                        haveAtLeastOneResult = True

                        if 'status' in result and  result['status'] in  ['disabled','unimplemented']:
                            testsWithoutResults.append(
                                dict( {'testName':result['testName'] }.items() + skeletonRecord.items()))

                        elif 'status' in result and  result['status'] in  ['pass','fail', 'beta']:
                            if 'queries' in result['testName'].lower():
                                pass #result['testName'] = ''

                            testsWithResults.append( result )
                            if result['pass']:
                                componentSummary[c]['passCount'] = componentSummary[c]['passCount'] + 1
                            else:
                                componentSummary[c]['failCount'] = componentSummary[c]['failCount'] + 1


                            #componentSummary[c]['elapsedTime'] = componentSummary[c]['elapsedTime'] + int(result['elapsedTime'])


                        else:
                            print 'unknown status', result
                            if 'reason' in result:
                                result['testMetric'] = result['reason'].replace('Have an error during workload generation','workload error')

                            testsWithoutResults.append( dict( {'notes':notes}.items() + result.items() + smallerSkeletonRecord.items()))

                if not haveAtLeastOneResult:
                    testsWithoutResults.append(
                                dict( {'testName':test['testName'],
                                       'testMetric':test['testMetric'] if 'testMetric' in test else '','notes':notes}.items()
                                         + smallerSkeletonRecord.items()))

        # massage the output
        for i in range(len(testsWithResults)):
            c = deepcopy( testsWithResults[i] )
            if i > 0 and testsWithResults[i-1]['testName'] == testsWithResults[i]['testName']:
                c['testName'] = ''
                c['elapsedTime'] = ''

            if i > 1 and testsWithResults[i-2]['testName'] == testsWithResults[i - 1 ]['testName'] and \
                testsWithResults[i-1]['testName'] != testsWithResults[i]['testName']:
                allTestResults.append(blankRecord)

            allTestResults.append( c )
        for i in testsWithoutResults: allTestResults.append( i )

        allTestResults.append(blankRecord)

    #print '\n\ntestsWithResults', testsWithResults


    sortedByComponent = list(componentSummary.get(i) for i in components)
    print '\n\n\n', format_as_table( sortedByComponent,  ['component', 'passCount','failCount'],
                               ['Component','Passing Tests','Failing Tests'] )



    if len(failingTests) > 0:
        print '\n\nFailing tests:\n'
        print format_as_table( sorted(failingTests, key=lambda k: k['testName']), ['testName','testMetric','expectedValue','actualValue','jira','notes'],
                               ['Test Name','Metric','Expected','Actual','Jira','Notes'] )






    # indent the test names
    for i in allTestResults:
        #print i
        if 'isAHeader' not in i:
            i['testName'] = '  ' + i['testName']
        if '-queries' in i['testName'].lower():
            i['testName'] = ''
        i['testName'] = i['testName'].replace('n1ql_thr_lat_','')
        if 'pass' in i and not i['pass']:
            pass #i['testMetric'] += '*'

        if options.historicalData:
            for h in historicalReleases:
                if h not in i: i[h] = ''
        if 'xdcr_init_1x1_unidir_1M_xdcr_1x1' in i['testMetric']: i['testMetric'] = 'throughput'



    print '\n\nFull report\n'

    if options.historicalData:
        historicalReleasesHeader = [i['release'] + '-' + i['build'] + '   ' for i in historicalResultDescriptor]

        print format_as_table( allTestResults, ['testName','testMetric'] + historicalReleases[0:-1] +
                               ['actualValue'] + [historicalReleases[-1]] + ['notes'], #,'elapsedTime'],
                                   ['Test Name','Metric'] + historicalReleasesHeader[0:-1] + [options.version] +
                                   [historicalReleasesHeader[-1]] + ['Notes']) #, 'Elapsed Time (min)'] )
    else:

        print format_as_table( allTestResults, ['testName','testMetric','expectedValue', 'actualValue','notes','elapsedTime'],
                                   ['Test Name','Metric','Expected', 'Actual', 'Notes', 'Elapsed Time (min)'] )



    if len(passedOnSecondRun) > 0:
        print '\n\nPassed on second run, these are the first run results:\n'
        print format_as_table( sorted(passedOnSecondRun, key=lambda k: k['testName']), ['testName','testMetric','expectedValue','actualValue'],
                               ['Test Name','Metric','Expected','Actual'] )


    if len(failedOnSecondRun) > 0:
        print '\n\nPassed on first run, failed on the second run, these are the second run results:\n'
        print format_as_table( sorted(failedOnSecondRun, key=lambda k: k['testName']), ['testName','testMetric','expectedValue','actualValue'],
                               ['Test Name','Metric','Expected','Actual'] )





    if len(passingTests) > 0:
        print '\n\nPassing tests:\n'
        print format_as_table( sorted(passingTests, key=lambda k: k['testName']), ['testName','testMetric','expectedValue','actualValue'],
                               ['Test Name','Metric','Expected','Actual'] )


    if len(filteredEnvironmentalIssues) > 0:
        print '\n\nEnvironment issues:\n'
        print format_as_table( filteredEnvironmentalIssues, ['testName','reason'],
                           ['Test Name','Reason'] )



    if len(stabilizingTests) > 0:
        print '\n\nTests being stabilized:\n'
        print format_as_table( sorted(stabilizingTests, key=lambda k: k['testName']), ['testName','testMetric','expectedValue','actualValue'],
                               ['Test Name','Metric','Expected','Actual'] )



if __name__ == "__main__":
    main()
