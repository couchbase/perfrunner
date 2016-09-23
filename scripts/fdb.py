import fileinput
import json
import os

import requests

benchmark = {
    'group': 'ForestDB',
    'metric': None,
    'value': None,
}


def set_documents(line):
    """Example:
        # documents (i.e. working set size): 80000000
    """
    num_docs = int(line.split()[6])
    benchmark['group'] += ': {}M docs'.format(num_docs / 10 ** 6)


def set_workload(line):
    """Example:
        write ratio: 80 % (synchronous)
    """
    ratio = int(line.split()[2])
    if ratio < 50:
        benchmark['group'] += ', read-heavy'
    elif ratio > 50:
        benchmark['group'] += ', write-heavy'
    else:
        benchmark['group'] += ', mixed'


def set_compaction(line):
    """Example: compaction threshold: 100 % (period: 60 seconds, auto)"""
    threshold = int(line.split()[2])
    benchmark['group'] += ', {}% compaction threshold'.format(threshold)


def set_cbr(line):
    """Example:
        block reuse threshold: 65 %
    """
    threshold = int(line.split()[3])
    benchmark['group'] += ', {}% CBR threshold'.format(threshold)


def set_throughput(line):
    """Example:
        34858349 reads (9682.83 ops/sec, 826.20 us/read)
        139437362 writes (38732.42 ops/sec, 103.27 us/write)
    """
    if 'writes' in line:
        benchmark['metric'] = 'Write throughput, ops/sec'
    elif 'reads' in line:
        benchmark['metric'] = 'Read throughput, ops/sec'
    else:
        return
    thr = line.split()[2].strip('(')
    benchmark['value'] = int(float(thr))
    post()


def post():
    host = os.environ.get('TRACKER', '127.0.0.1:8080')
    data = json.dumps(benchmark, indent=4, sort_keys=True)

    print('Posting: {}'.format(data))
    requests.post('http://{}/api/v1/benchmarks'.format(host), data=data)


def main():
    for line in fileinput.input():
        # Group attributes
        if 'working set size' in line:
            set_documents(line)

        if 'write ratio' in line:
            set_workload(line)
        elif 'compaction threshold' in line:
            set_compaction(line)
        elif 'block reuse threshold' in line:
            set_cbr(line)

        # Metric attributes
        elif 'ops/sec' in line:
            set_throughput(line)


if __name__ == '__main__':
    main()
