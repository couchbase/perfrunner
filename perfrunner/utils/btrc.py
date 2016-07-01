import json
import sys
from optparse import OptionParser

import requests
from logger import logger


class CouchbaseClient(object):

    """Minimal Couchbase client
    """

    def __init__(self, host_port, bucket, username, password):
        self.base_url = 'http://{0}'.format(host_port)
        self.bucket = bucket
        self.auth = (username, password)

    def safe_get(self, url):
        try:
            return requests.get(url=url, auth=self.auth).json()
        except (ValueError, requests.exceptions.ConnectionError) as e:
            logger.warn(e)
            return {}

    def list_of_nodes(self):
        url = self.base_url + '/pools/default/'
        data = self.safe_get(url=url)

        if data:
            for node in data['nodes']:
                hostname, port = node['hostname'].split(':')
                if port == '8091':
                    yield hostname + ':8092'
                else:
                    yield hostname + ':9500'
        elif data is None:
            sys.exit('Node has no buckets/misconfigured')
        else:
            sys.exit('Cannot establish connection with specified [host:port] '
                     'node')

    def list_of_ddocs(self):
        url = self.base_url + \
            '/pools/default/buckets/{0}/ddocs'.format(self.bucket)
        data = self.safe_get(url=url)

        if data:
            for row in data['rows']:
                yield row['doc']['meta']['id']
        elif data is None:
            sys.exit('Wrong bucket name')

    def set_view_params(self):
        for node in self.list_of_nodes():
            for ddoc in self.list_of_ddocs():
                url = 'http://{0}'.format(node) + \
                      '/_set_view/{0}/{1}/'.format(self.bucket, ddoc)
                yield node, ddoc, url

    def get_btree_stats(self):
        for node, ddoc, url in self.set_view_params():
            url += '_btree_stats'
            data = self.safe_get(url)
            if data:
                yield node, ddoc, data

    def get_utilization_stats(self):
        for node, ddoc, url in self.set_view_params():
            url += '_get_utilization_stats'
            data = self.safe_get(url)
            if data:
                yield node, ddoc, data

    def reset_utilization_stats(self):
        for _, _, url in self.set_view_params():
            url += '_reset_utilization_stats'
            requests.post(url=url, auth=self.auth)


class CliArgs(object):

    """CLI options and args handler
    """

    def __init__(self):
        usage = 'usage: %prog -n node:port [-b bucket] -c command \n\n' +\
                'Example: %prog -n 127.0.0.1:8091 ' +\
                '-u Administrator -p password -b default -c btree_stats'

        parser = OptionParser(usage)

        parser.add_option('-n', dest='host_port',
                          help='Node address', metavar='127.0.0.1:8091')
        parser.add_option('-u', dest='username',
                          help='REST username', metavar='Administrator')
        parser.add_option('-p', dest='password',
                          help='REST password', metavar='password')
        parser.add_option('-b', dest='bucket', default='default',
                          help='Bucket name', metavar='default')
        parser.add_option('-c', dest='command',
                          help='Stats command', metavar='command')

        self.options, self.args = parser.parse_args()
        self.validate_options(parser)

    def validate_options(self, parser):
        if not self.options.host_port:
            parser.error('Missing node address [-n]')
        if not self.options.command:
            parser.error('Missing command [-c]')
        if self.options.command not in ('btree_stats', 'util_stats', 'reset'):
            parser.error('Only "btree_stats", "util_stats" and "reset" '
                         'commands supported')


class StatsReporter(object):

    """Save all stats in *.json files
    """

    def __init__(self, cb):
        self.cb = cb

    def report_stats(self, stats_type):
        if stats_type == 'btree_stats':
            stats_generator = self.cb.get_btree_stats
        else:
            stats_generator = self.cb.get_utilization_stats

        for node, ddoc, stat in stats_generator():
            filename = '{0}_{1}{2}.json'.format(stats_type,
                                                node.replace(':', '_'),
                                                ddoc.replace('/', '_'))
            with open(filename, 'w') as fh:
                logger.info('Saving {0} stats to: {1}'.format(stats_type,
                                                              filename))
                fh.write(json.dumps(stat, indent=4, sort_keys=True))


def main():
    ca = CliArgs()
    cb = CouchbaseClient(ca.options.host_port, ca.options.bucket,
                         ca.options.username, ca.options.password)
    reporter = StatsReporter(cb)

    if ca.options.command in ('btree_stats', 'util_stats'):
        reporter.report_stats(ca.options.command)
    elif ca.options.command == 'reset':
        cb.reset_utilization_stats()

if __name__ == '__main__':
    main()
