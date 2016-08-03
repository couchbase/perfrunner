import json
import fileinput
import os

import requests


benchmark = {
    'group': 'ForestDB, {} workload',
    'metric': None,
    'value': None,
}


def set_title(line):
    ratio = int(line.split()[2])
    if ratio < 50:
        benchmark['group'] = benchmark['group'].format('Read-heavy')
    if ratio > 50:
        benchmark['group'] = benchmark['group'].format('Write-heavy')
    else:
        benchmark['group'] = benchmark['group'].format('Mixed')


def post():
    host = os.environ.get('TRACKER', '127.0.0.1:8080')
    data = json.dumps(benchmark, indent=4, sort_keys=True)

    print('Posting: {}'.format(data))
    requests.post('http://{}/api/v1/benchmarks'.format(host), data=data)


def main():
    for line in fileinput.input():
        if 'write ratio' in line:
            set_title(line)

        if 'ops/sec' in line:
            if 'writes' in line:
                benchmark['metric'] = 'Write throughput, ops/sec'
            elif 'reads' in line:
                benchmark['metric'] = 'Read throughput, ops/sec'
            else:
                continue
            thr = line.strip().split()[2].strip('(')
            benchmark['value'] = int(float(thr))
            post()


if __name__ == '__main__':
    main()
