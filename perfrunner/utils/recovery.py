from argparse import ArgumentParser
from multiprocessing import set_start_method

from logger import logger
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec

set_start_method("fork")


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster', dest='cluster_spec_fname',
                        required=True,
                        help='path to the cluster specification file')

    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname)

    remote = RemoteHelper(cluster_spec, verbose=False)

    logger.info('Recovering system state')
    for host, version in remote.get_system_backup_version().items():
        remote.start_system_state_recovery(host, version)


if __name__ == '__main__':
    main()
