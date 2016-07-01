from argparse import ArgumentParser

from spring.settings import WorkloadSettings, TargetSettings
from spring.version import VERSION
from spring.wgen import WorkloadGen


class CLIParser(ArgumentParser):

    PROG = 'spring'
    USAGE = (
        '%(prog)s [-crud PERCENTAGE] [-o #OPS] [-i #ITEMS] [-n #WORKERS] '
        '[cb://user:pass@host:port/bucket]')

    def __init__(self):
        super(CLIParser, self).__init__(prog=self.PROG, usage=self.USAGE)
        self._add_arguments()

    def _add_arguments(self):
        self.add_argument(
            'uri', metavar='URI', nargs='?',
            default='cb://127.0.0.1:8091/default',
            help='Connection URI'
        )
        self.add_argument(
            '-v', '--version', action='version', version=VERSION
        )
        self.add_argument(
            '-c', dest='creates', type=int, default=0, metavar='',
            help='percentage of "create" operations (0 by default)',
        )
        self.add_argument(
            '-r', dest='reads', type=int, default=0, metavar='',
            help='percentage of "read" operations (0 by default)',
        )
        self.add_argument(
            '-u', dest='updates', type=int, default=0, metavar='',
            help='percentage of "update" operations (0 by default)',
        )
        self.add_argument(
            '-d', dest='deletes', type=int, default=0, metavar='',
            help='percentage of "delete" operations (0 by default)',
        )
        self.add_argument(
            '-e', dest='expiration', type=int, default=0, metavar='',
            help='percentage of new items that expire (0 by default)',
        )
        self.add_argument(
            '-o', dest='ops', type=int, default=float('inf'), metavar='',
            help='total number of operations (infinity by default)'
        )
        self.add_argument(
            '-t', dest='throughput', type=int, default=float('inf'),
            metavar='',
            help='target operations throughput (infinity by default)'
        )
        self.add_argument(
            '-s', dest='size', type=int, default=2048, metavar='',
            help='average value size in bytes (2048 by default)'
        )
        self.add_argument(
            '-p', dest='prefix', type=str, default='', metavar='',
            help='key prefix (no prefix by default)'
        )
        self.add_argument(
            '-i', dest='items', type=int, default=0, metavar='',
            help='number of existing items (0 by default)',
        )
        self.add_argument(
            '-w', dest='working_set', type=int, default=100, metavar='',
            help='percentage of items in working set, 100 by default'
        )
        self.add_argument(
            '-W', dest='working_set_access', type=int, default=100, metavar='',
            help=('percentage of operations that hit working set, '
                  '100 by default')
        )
        self.add_argument(
            '-n', dest='workers', type=int, default=1, metavar='',
            help='number of workers (1 by default)'
        )
        self.add_argument(
            '-g', dest='generator', type=str, default='old', metavar='',
            choices=('old', 'new'),
            help='document generator ("old" or "new")'
        )
        self.add_argument('--async', action='store_true', default=False,
                          help='enable asynchronous mode')
        self.add_argument(
            '--data', dest='data', type=str, default='', metavar='',
            help='file to use as input (instead of a document generator)'
        )
        self.add_argument(
            '-m', dest='dimensionality', type=int, default=0, metavar='',
            help='dimensionality of the data',
        )

    def parse_args(self, *args):
        args = super(CLIParser, self).parse_args()

        percentages = [args.creates, args.reads, args.updates, args.deletes]
        if filter(lambda p: not 0 <= p <= 100, percentages) or \
                sum(percentages) != 100:
            self.error('Invalid operation [-c, -r, -u, -d] percentage')

        if not 0 <= args.working_set <= 100:
            self.error('Invalid working set [-w] percentage.')

        if not 0 <= args.expiration <= 100:
            self.error('Invalid expiration [-e] percentage.')

        if not 0 <= args.working_set_access <= 100:
            self.error('Invalid access percentage [-W].')

        if (args.reads or args.updates) and not args.items:
            self.error('Trying to read/update indefinite dataset. '
                       'Please specify number of items in dataset (-i)')

        return args


def main():
    parser = CLIParser()
    args = parser.parse_args()

    ws = WorkloadSettings(args)
    ts = TargetSettings(args.uri, args.prefix)
    wg = WorkloadGen(ws, ts)
    wg.run()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
