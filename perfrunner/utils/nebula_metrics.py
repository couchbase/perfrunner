import json
import os
import signal
import subprocess
import zipfile
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta

import requests
from requests import HTTPError

from logger import logger
from perfrunner.settings import ClusterSpec

SERVICES = ['nebula', 'dataApi']

PROMETHEUS_PORT = 9090
BASE_URL = 'http://localhost:{}'.format(PROMETHEUS_PORT)
PROMTHEUS_POD = 'prometheus-serverless-0'
OUTPUT_ZIPFILE = 'nebula_metrics.zip'

QUERY_TEMPLATE = (
    '{{__name__!~"|serverless_node_metadata",job="Serverless Clusters",databaseId="{cluster_id}"}} '
    'and on (databaseId,couchbaseNode) '
    'serverless_node_metadata{{serverlessService="{service}",job="Serverless Clusters"}}'
)


def download_metrics(cluster_id: str, service: str, start: datetime, end: datetime,
                     filename: str) -> bool:
    logger.info('Downloading prometheus metrics for {}'.format(service))

    api = '{}/api/v1/query_range'.format(BASE_URL)
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'query': QUERY_TEMPLATE.format(cluster_id=cluster_id, service=service),
        'start': int(start.timestamp()),
        'end': int(end.timestamp()),
        'step': '15s'
    }

    found_data = False

    with requests.post(api, data=data, headers=headers, stream=True) as r:
        try:
            r.raise_for_status()
        except HTTPError as e:
            logger.error('Failed to download metrics for {}'.format(service))
            logger.error('HTTP error: {}'.format(e))
            return False

        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                found_data = True

    if not found_data:
        logger.error('No metrics were found for {}'.format(service))

    return found_data


def convert_to_openmetrics(in_file: str, out_file: str):
    with open(in_file, 'r') as f:
        data = json.load(f)['data']['result']

    with open(out_file, 'w') as f:
        while data:
            row_common = data[0]['metric']
            for t, v in data[0]['values']:
                name = row_common['__name__']
                labels = ','.join(
                    '{}="{}"'.format(k, v) for k, v in row_common.items() if k != '__name__'
                )
                full_metric = '{}{{{}}} {} {}\n'.format(name, labels, v, t)
                f.write(full_metric)

            data.pop(0)
        f.write('# EOF')


def update_kubeconfig(sandbox: str) -> int:
    logger.info('Updating kubeconfig')
    command = (
        'env/bin/aws eks --region us-east-1 '
        'update-kubeconfig --name {0}-cp-eks --alias {0}'
    ).format(sandbox).split()
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if (returncode := proc.returncode) != 0:
        logger.error('Failed to update kubeconfig using command: {}'.format(command))
        logger.error('Command stdout: {}'.format(stdout))
        logger.error('Command stderr: {}'.format(stderr))
    return returncode


def start_port_forwarding() -> subprocess.Popen:
    command = 'kubectl port-forward --address 127.0.0.1 {0} {1}:{1}'\
              .format(PROMTHEUS_POD, PROMETHEUS_PORT).split()

    logger.info('Port forwarding to {} pod'.format(PROMTHEUS_POD))
    portforward = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    logger.info('Waiting for port forwarding to initialise')

    # Wait for first line of stdout to ensure port forwarding is set up before continuing
    portforward.stdout.readline()

    return portforward


def move_metrics_to_archive(json_file: str, openmetrics_file: str, archive: str) -> None:
    logger.info('Moving {}, {} to archive {}'
                .format(json_file, openmetrics_file, archive))

    for fn in [json_file, openmetrics_file]:
        if not os.path.exists(fn):
            logger.error('File "{}" not found. Cannot move to archive "{}"'.format(fn, archive))
            return

    with zipfile.ZipFile(archive, 'a', compression=zipfile.ZIP_DEFLATED) as z:
        z.write(json_file)
        z.write(openmetrics_file)

    logger.info('Metrics added to archive.')

    logger.info('Removing {}, {}'.format(json_file, openmetrics_file))
    os.remove(json_file)
    os.remove(openmetrics_file)


def get_args() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a infrastructure specification file')

    return parser.parse_args()


def main():
    args = get_args()
    spec = ClusterSpec()
    spec.parse(args.cluster)

    cluster_id = spec.infrastructure_settings.get('cbc_cluster')

    end = datetime.now()
    start = datetime.now() - timedelta(hours=24)

    sandbox = spec.infrastructure_settings.get('cbc_env').split('.')[0]
    if update_kubeconfig(sandbox) != 0:
        exit(1)

    portforward = start_port_forwarding()

    try:
        for service in SERVICES:
            json_file = '{}.json'.format(service)
            openmetrics_file = '{}.openmetrics.txt'.format(service)

            if download_metrics(cluster_id, service, start, end, json_file):
                try:
                    convert_to_openmetrics(json_file, openmetrics_file)
                except Exception as e:
                    logger.error('Failed to convert {} metrics to OpenMetrics format'
                                 .format(service))
                    logger.error('Exception raised: {}'.format(e))

                move_metrics_to_archive(json_file, openmetrics_file, OUTPUT_ZIPFILE)
    finally:
        logger.info('Terminating port forwarding')
        portforward.send_signal(signal.SIGTERM)


if __name__ == '__main__':
    main()
