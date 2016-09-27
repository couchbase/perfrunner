from optparse import OptionParser

from logger import logger

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec


def get_options():
    usage = '%prog -c cluster'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to the cluster specification file',
                      metavar='cluster.spec')

    options, args = parser.parse_args()
    if not options.cluster_spec_fname:
        parser.error('Please specify a cluster specification')

    return options, args


def main():
    options, args = get_options()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, args)

    remote = RemoteHelper(cluster_spec, test_config=None, verbose=False)

    logger.info('Recovering system state')
    for host, version in remote.get_system_backup_version().items():
        remote.start_system_state_recovery(host, version)


if __name__ == '__main__':
    main()
