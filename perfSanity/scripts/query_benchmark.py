import requests
import json
import re
import fileinput

from optparse import OptionParser
import subprocess
import os
import sys
import time

from perfrunner.settings import ClusterSpec
from perfrunner.utils.install import CouchbaseInstaller
from perfrunner.utils.cluster import TestConfig, ClusterManager

from logger import logger
import urllib3
from perfrunner.helpers.rest import RestHelper
import paramiko


#from couchbase.bucket import Bucket
#import couchbase
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError


"""
# An evolving thing - takes as input:
- a build version
- a spec file
What it does:
   - install the spec file on the build version
   - activate the beer sample bucket
   - run the tests from Keshav -flag an error if they deviate
"""

UPPER_BOUND = 1.10
LOWER_BOUND = 0.90

ARGS = None
# global variables - yuck
version = None
runStartTime = None
couchbaseConnection = None

def get_time_in_millisec(t):
    try:
        time_unit = t[-2:]
        if time_unit == 'ms':
            return float(t[:-2])
        elif time_unit == u"\u00b5s":
            return float(t[:-2]) / 1000
        elif 'm' in t and 'ms' not in t:
            t1 = t.split('m')
            return int(t1[0]) * 60000 + float(t1[1][:-1]) * 1000
        elif time_unit[0].isdigit and time_unit[1] == 's':
            return float(t[:-1]) * 1000
        else:
            print '********unknown time unit', t
    except:
        print 'bad time', t


def generate_query(stmt):
    stmt['max_parallelism'] = 1
    if ARGS:
        stmt['args'] = ARGS
    return stmt


def generate_prepared_query(conn, q):
    query = {'statement': 'PREPARE ' + q, 'max_parallelism': 1}

    response = conn.request('POST', '/query/service', fields=query, encode_multipart=False)
    body = json.loads(response.data.decode('utf8'))
    name = str(body['results'][0]['name'])
    stmt = {'prepared': '"' + name + '"'}
    return generate_query(stmt)


def run_query(conn, request_desc, debug=False):
    succeeded = True

    query = generate_prepared_query(conn, request_desc['query'])
    total_elapsed_time = 0.0
    total_execution_time = 0.0
    for i in range(0, request_desc['execution_count']):
        """if debug:
            #t0 = time.time()"""
        response = conn.request('POST', '/query/service', fields=query, encode_multipart=False)
        response.read(cache_content=False)
        body = json.loads(response.data.decode('utf8'))
        total_elapsed_time = total_elapsed_time + get_time_in_millisec(body['metrics']['elapsedTime'])
        total_execution_time = total_execution_time + get_time_in_millisec(body['metrics']['executionTime'])

    avg_elapsed = float('{0:.2f}'.format(total_elapsed_time / request_desc['execution_count']))
    avg_execution = float('{0:.2f}'.format(total_execution_time / request_desc['execution_count']))
    log = 'Query {0} - average elapsed {1}, average execution time {2}.'.format(request_desc['query'], avg_elapsed,
                                                                                avg_execution)

    if avg_elapsed > (UPPER_BOUND * request_desc['expected_elapsed_time']):
        log += ' Elapsed too long - expected {0}.'.format(avg_elapsed)
        succeeded = False

    if avg_execution > (UPPER_BOUND * request_desc['expected_execution_time']):
        log += ' Execution too long - expected {0}.'.format(avg_execution)
        succeeded = False

    if avg_elapsed < (LOWER_BOUND * request_desc['expected_elapsed_time']):
        log += ' Elapsed too short - expected {0}.'.format(avg_elapsed)
        succeeded = False

    if avg_execution < (LOWER_BOUND * request_desc['expected_execution_time']):
        log += ' Execution too short - expected {0}.'.format(avg_execution)
        succeeded = False

    if succeeded:
        logger.info(log)
    else:
        logger.error(log)

    return succeeded


def execute_commands(conn, command_list, rest, host_ip, testName):
    global couchbaseConnection, version, runStartTime
    failure_count = 0

    for command in command_list:
        # print 'command', command
        command_succeeded = True
        total_elapsed_time = 0.0
        total_execution_time = 0.0

        if 'index' in command:
            key = 'index'
            response = rest.exec_n1ql_stmnt(host_ip, command['index'])
            body = response.json()  # json.loads(response.data.decode('utf8'))
            avg_elapsed = total_elapsed_time + get_time_in_millisec(body['metrics']['elapsedTime'])
            avg_execution = total_execution_time + get_time_in_millisec(body['metrics']['executionTime'])
        elif 'query' in command:
            key = 'query'
            query = generate_prepared_query(conn, command['query'])
            for i in range(0, command['execution_count']):
                response = conn.request('POST', '/query/service', fields=query, encode_multipart=False)
                response.read(cache_content=False)
                body = json.loads(response.data.decode('utf8'))
                total_elapsed_time = total_elapsed_time + get_time_in_millisec(body['metrics']['elapsedTime'])
                total_execution_time = total_execution_time + get_time_in_millisec(body['metrics']['executionTime'])
            avg_elapsed = float('{0:.2f}'.format(total_elapsed_time / command['execution_count']))
            avg_execution = float('{0:.2f}'.format(total_execution_time / command['execution_count']))
            log = key + ' {0} - average elapsed {1}, average execution time {2}.'.format(command[key], avg_elapsed,
                                                                                         avg_execution)
            if avg_elapsed > (UPPER_BOUND * command['expected_elapsed_time']):
                log += ' Elapsed too long - expected {0}.'.format(command['expected_elapsed_time'])
                command_succeeded = False
            elif avg_elapsed < (LOWER_BOUND * command['expected_elapsed_time']):
                log += ' Elapsed too short - expected {0}.'.format(command['expected_elapsed_time'])
                command_succeeded = False
            if avg_execution > (UPPER_BOUND * command['expected_execution_time']):
                log += ' Execution too long - expected {0}.'.format(command['expected_execution_time'])
                command_succeeded = False
            elif avg_execution < (LOWER_BOUND * command['expected_execution_time']):
                log += ' Execution too short - expected {0}.'.format(command['expected_execution_time'])
                command_succeeded = False

            if command_succeeded:
                logger.info(log)
            else:
                failure_count = failure_count + 1
                logger.error(log)


	    val = {
  		"actualValue": avg_execution,
 		 "expectedValue": command['expected_execution_time'],
  		"build": version,
  		"runStartTime": runStartTime,
  		"pass": command_succeeded,
  		"testMetric": command['queryDesc'],
  		"testStartTime": time.strftime("%m/%d/%y-%H:%M:%S", time.strptime(time.ctime() )),
  		"testName": testName
		}
            key = runStartTime + '-' + testName +'-' + command['queryDesc']
            couchbaseConnection.add(key, val)
    return failure_count == 0


def do_beer_queries(conn, rest, host_ip, remote):
    remote.install_beer_samples()

    rest.exec_n1ql_stmnt(host_ip, 'CREATE INDEX city ON `beer-sample`(city);')
    rest.exec_n1ql_stmnt(host_ip, 'CREATE INDEX style ON `beer-sample`(style);')

    command_list = []
    command_list.append(
        {'queryDesc':'select *', 'query': 'SELECT * FROM `beer-sample` USE KEYS["21st_amendment_brewery_cafe-amendment_pale_ale"];',
         'expected_elapsed_time': 0.7, 'expected_execution_time': 0.7, 'execution_count': 10000})
    command_list.append({'queryDesc':'select * with where', 'query': 'select * from `beer-sample` where city = "Lawton";', 'expected_elapsed_time': 1.42,
                         'expected_execution_time': 1.42, 'execution_count': 10000})
    command_list.append(
        {'queryDesc':'select * where with or', 'query': 'select abv, brewery_id from `beer-sample` where style =  "Imperial or Double India Pale Ale";',
         'expected_elapsed_time': 11,
         'expected_execution_time': 11, 'execution_count': 10000})
    command_list.append(
        {'queryDesc':'select count with where', 'query': 'select COUNT(*) from `beer-sample` where style =  "Imperial or Double India Pale Ale";',
         'expected_elapsed_time': 1.5, 'expected_execution_time': 1.5, 'execution_count': 10000})
    command_list.append(
        {'queryDesc':'select sum where', 'query': 'select SUM(abv) from `beer-sample` where style =  "Imperial or Double India Pale Ale";',
         'expected_elapsed_time': 11, 'expected_execution_time': 11, 'execution_count': 10000})
    command_list.append({
        'queryDesc':'select fields order by', 'query': 'select abv, brewery_id from `beer-sample` where style =  "Imperial or Double India Pale Ale" order by abv;',
        'expected_elapsed_time': 15.4, 'expected_execution_time': 15.4, 'execution_count': 10000})

    return execute_commands(conn, command_list, rest, host_ip, 'beer-queries')


def do_airline_benchmarks(conn, rest, host_ip, remote, cluster_spec):
    if True:
        resp = rest.create_bucket(host_ip + ':8091', 'ods', 1000, 0, 0, 'valueOnly', 4, None)
        time.sleep(10)

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(host_ip, username=cluster_spec.ssh_credentials[0], password=cluster_spec.ssh_credentials[1])
        except paramiko.SSHException:
            print "ssh Connection Failed"
            return False

        cmd = '/opt/couchbase/bin/cbrestore /root/airline-test-data-updated  couchbase://127.0.0.1:8091 -b ods -B ods -u {0} -p {1}'.format(
            rest.rest_username, rest.rest_password)
        stdin, stdout, stderr = ssh.exec_command(cmd)

        for line in stdout.readlines():
            pass
        ssh.close()

    command_list = []
    command_list.append(
        {'index': 'create primary index on ods;', 'expected_elapsed_time': 27000, 'expected_execution_time': 27000})
    command_list.append(
        {'index': 'CREATE INDEX IDX_ODS_TAIL_NBR ON ods(`TAIL_NBR`) WHERE (`TYPE` = "OPS_FLT_LEG") USING GSI;',
         'expected_elapsed_time': 38000, 'expected_execution_time': 38000})
    command_list.append(
        {'queryDesc':'Q1', 'query': "SELECT * FROM   ods WHERE  TYPE = 'OPS_FLT_LEG' AND TAIL_NBR = 'N518LR' ORDER  BY GMT_EST_DEP_DTM ;",
         'expected_elapsed_time': 1.3, 'expected_execution_time': 1.3, 'execution_count': 10})

    # query 2
    big_long_query2 = """
    select
        pilot.FILEN as pilot_filen,
        min([p.PRFL_ACT_GMT_DEP_DTM, meta(p).id]) PRFL_ACT_GMT_DEP_DTM
    from
    (  SELECT x.*
       FROM ods x
           where x.TYPE="CREW_ON_FLIGHT" AND
           (
               x.PRFL_ACT_GMT_DEP_DTM <= "2015-07-23T18:49:00Z"
           )
    ) as p unnest array_concat(p.PILOT,p.CREW) pilot
    WHERE
    pilot.FILEN in (
        select raw pilot1.FILEN
        from ods f use keys [ "UA_22-07-2015_EWR_IAD_6049" ]
        unnest array_concat(f.PILOT,f.CREW) pilot1
    )
    group by pilot.FILEN
    UNION ALL
    select
        pilot.FILEN as pilot_filen,
        min([p.GMT_EST_DEP_DTM, meta(p).id]) GMT_EST_DEP_DTM
    from
    (
        SELECT y.*
        FROM ods y
        where y.TYPE="CREW_ON_FLIGHT" AND
        (
            y.GMT_EST_DEP_DTM <= "2015-07-23T18:49:00Z"
        )
    ) as p unnest array_concat(y.PILOT,y.CREW) pilot
    where
    pilot.FILEN in (
        select raw pilot1.FILEN
        from ods f use keys [ "UA_22-07-2015_EWR_IAD_6049" ]
        unnest array_concat(f.PILOT,f.CREW) pilot1
    )"""


    """
    MB-18839
    command_list.append({
        'index': 'CREATE INDEX IDX_GMT_EST_DEP_DTM ON ods(`GMT_EST_DEP_DTM`) WHERE (`TYPE`="CREW_ON_FLIGHT") USING GSI;',
        'expected_elapsed_time': 38000, 'expected_execution_time': 38000})
    command_list.append({
        'index': 'CREATE INDEX IDX_PRFL_ACT_GMT_DEP_DTM ON ods(`PRFL_ACT_GMT_DEP_DTM`) WHERE (`TYPE`="CREW_ON_FLIGHT") USING GSI;',
        'expected_elapsed_time': 41000, 'expected_execution_time': 41000})
    command_list.append(
        {'queryDesc':'Q2', 'query': big_long_query2, 'expected_elapsed_time': 536, 'expected_execution_time': 536, 'execution_count': 10})
    """

    # query 3
    big_long_index3 = """
    create index idx_query3 on ods(INBND_LCL_EST_ARR_DTM)
    where TYPE="AIRCRAFT_ROUTING"
    and substr(INBND_LCL_EST_ARR_DTM, 11) < "20:00:00"
    and case when OUTBND_LCL_EST_DEP_DTM is missing then true else substr(OUTBND_LCL_EST_DEP_DTM, 11) > "08:00:00" end;
    """

    big_long_query3 = """
    SELECT    INBND_DEST_ARPT_CD
    from ods
    where TYPE = "AIRCRAFT_ROUTING"
    and  INBND_LCL_EST_ARR_DTM > "2015-07-17"
    and  INBND_LCL_EST_ARR_DTM < "2015-07-25"
    and  substr(INBND_LCL_EST_ARR_DTM, 11) < "20:00:00"
    and case when OUTBND_LCL_EST_DEP_DTM is missing then true else substr(OUTBND_LCL_EST_DEP_DTM, 11) > "08:00:00" end
    order by INBND_DEST_ARPT_CD
    limit 10;
    """

    command_list.append({'index': big_long_index3, 'expected_elapsed_time': 64000, 'expected_execution_time': 64000})
    command_list.append({'queryDesc':'Q3', 'query': big_long_query3, 'expected_elapsed_time': 2300, 'expected_execution_time': 2300,
                         'execution_count': 10})

    return execute_commands(conn, command_list, rest, host_ip, 'United-Queries')

def do_sabre_benchmarks(conn, rest, host_ip, remote, cluster_spec):
    if True:
        resp = rest.create_bucket(host_ip + ':8091', 'sabre', 1000, 0, 0, 'valueOnly', 4, None)
        time.sleep(10)

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(host_ip, username=cluster_spec.ssh_credentials[0], password=cluster_spec.ssh_credentials[1])
        except paramiko.SSHException:
            print "ssh Connection Failed"
            return False

        cmd = '/opt/couchbase/bin/cbrestore /root/sabre/backup  couchbase://127.0.0.1:8091 -b sabre -B sabre -u {0} -p {1}'.format(
            rest.rest_username, rest.rest_password)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        for line in stdout.readlines():
            pass
        ssh.close()

    command_list = []
    command_list.append(
            {'index': 'create index outbound_flight '
                      'on sabre (AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode,'
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode,'
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime);',
             'expected_elapsed_time': 27000, 'expected_execution_time': 27000})

    # query 1
    select_query1 = """
    SELECT
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode as Arrival_Airport,
    ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) as Flight_Segment,
    DATE_DIFF_STR(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,"hour") as Flight_Time,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime as Depart_Time,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime as Arrival_Time
    FROM sabre
    WHERE
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = 'SFO'
    and AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = 'BOS'
    and AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime ="2014-05-23T06:15:00"
    ORDER BY Flight_Time, Depart_Time, Arrival_Time
    LIMIT 10"""

    command_list.append(
            {'queryDesc':'Q1', 'query': select_query1, 'expected_elapsed_time':   434.0, 'expected_execution_time':   434.0, 'execution_count': 10})

    command_list.append(
            {'index': 'create index airiternary '
                      'on sabre(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode, '
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode, '
                      'AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount,'
                      'ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment),'
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,'
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime );',
             'expected_elapsed_time': 27000, 'expected_execution_time': 27000})
    # query 2
    select_query2 = """
    select AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as D, AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode as A, AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount as F,
    ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) as FS,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime as Arrival_Time,
     AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime as Depart_Time,
    DATE_DIFF_STR(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime,
        AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,"hour")
    from sabre
    where
        AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = 'SFO' and
        AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = 'MIA' and
        AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount between 400 and 450 and
        ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) < 3 and
       AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime is not null and
       AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime is not null
    order by F, D, A
    limit 10"""

    #command_list.append(
    #        {'queryDesc':'Q2', 'query': select_query2, 'expected_elapsed_time': 1.0, 'expected_execution_time': 1.0, 'execution_count': 10})

    #command_list.append(
    #        {'index': 'create index min_price_min_stop '
    #                  'on sabre (AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode,'
    #                  'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode,'
    #                  'ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment),'
    #                  'AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount);',
    #         'expected_elapsed_time': 27000, 'expected_execution_time': 27000})

    # query 3
    select_query3 = """
    SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode as Arrival_Airport,
    ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) as Flight_Segment,
    DATE_DIFF_STR(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,"hour") as Flight_Time,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime as Depart_Time,
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime as Arrival_Time,
    AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount as Fare
    FROM sabre
    WHERE
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = 'SFO' and
    AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = 'MIA' and
    AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount between 400 and 450
    ORDER BY Fare, Flight_Segment, Depart_Time, Arrival_Time
    LIMIT 10"""

    #command_list.append(
    #        {'queryDesc':'Q3', 'query': select_query3, 'expected_elapsed_time':  1.09, 'expected_execution_time':  1.09, 'execution_count': 10})

    #command_list.append(
    #        {'index': 'create index one_way_direction '
    #                  'on sabre (AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode,'
    #                  'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode,'
    #                  'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,'
    #                  'AirItinerary.DirectionInd) ;',
    #         'expected_elapsed_time': 27000, 'expected_execution_time': 27000})
    command_list.append(
            {'index': 'create index flight_direction '
                      'on sabre( AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,'
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].FlightNumber,'
                      'AirItinerary.DirectionInd);',
             'expected_elapsed_time': 27000, 'expected_execution_time': 27000})
    command_list.append(
            {'index': 'create index tax_sequence '
                      'on sabre(AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.Taxes.Tax[0].TaxCode,'
                      'SequenceNumber) ;',
             'expected_elapsed_time': 27000, 'expected_execution_time': 27000})
    command_list.append(
            {'index': 'create index flight_num_route '
                      'on sabre(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode,'
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode,'
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime, '
                      'AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].FlightNumber);',
             'expected_elapsed_time': 27000, 'expected_execution_time': 27000})

    return execute_commands(conn, command_list, rest, host_ip, 'Sabre-Queries')


def main():

    global couchbaseConnection, version, runStartTime

    usage = '%prog -v version -c cluster-spec'
    parser = OptionParser(usage)
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-r', '--runStartTime', dest='runStartTime')
    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')
    parser.add_option('-o', dest='toy',
                      help='optional toy build ID', metavar='couchstore')

    parser.add_option('-t', dest='test_config_fname',
                      help='path to test configuration file',
                      metavar='my_test.test')

    parser.add_option('-e', '--edition', dest='cluster_edition', default='enterprise',
                      help='the cluster edition (community or enterprise)')
    parser.add_option('--url', dest='url', default=None,
                      help='The http URL to a Couchbase RPM that should be'
                           ' installed.  This overrides the URL to be installed.')
    options, args = parser.parse_args()
    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, args)

    test_config = TestConfig()
    test_config.parse(options.test_config_fname, args)

    cm = ClusterManager(cluster_spec, test_config, options.verbose)


    couchbaseConnection = Couchbase.connect(bucket='Daily-Performance', host='172.23.105.177')

    runStartTime = options.runStartTime
    version = options.version
    #bucket = Bucket('couchbase://'+ '172.23.105.177:8091/Daily-Performance')

    installer = CouchbaseInstaller(cluster_spec, options)
    if True:
        installer.install()
        if cm.remote:
            cm.tune_logging()
            cm.restart_with_sfwi()
            cm.restart_with_alternative_num_vbuckets()
            cm.restart_with_alternative_num_cpus()
            cm.restart_with_tcmalloc_aggressive_decommit()
            cm.disable_moxi()
        cm.configure_internal_settings()
        cm.set_data_path()
        cm.set_services()
        cm.set_mem_quota()
        cm.set_index_mem_quota()
        cm.set_auth()
        time.sleep(30)
        """host = cluster_spec.yield_masters().next()"""
    host_ip = cluster_spec.yield_masters().next().split(':')[0]
    URL = 'http://' + host_ip + ':8093'
    logger.info('logging the URL: {}'.format(URL))
    conn = urllib3.connection_from_url(URL)
    rest = RestHelper(cluster_spec)
    cm.set_index_settings()
    airline_result = do_airline_benchmarks(conn, rest, host_ip, installer.remote, cluster_spec)
    beer_result = do_beer_queries(conn, rest, host_ip, installer.remote)
    print 'beer_result is', beer_result
    sabre_result = do_sabre_benchmarks(conn, rest, host_ip, installer.remote, cluster_spec)
    sys.exit(not (airline_result and beer_result and sabre_result))


if __name__ == "__main__":
    if not main():
        sys.exit(1)