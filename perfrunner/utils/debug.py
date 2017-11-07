import glob
import os.path
import shutil
from argparse import ArgumentParser

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec


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

    remote.collect_info()
    for hostname in cluster_spec.servers:
        for fname in glob.glob('{}/*.zip'.format(hostname)):
            shutil.move(fname, '{}.zip'.format(hostname))

    if cluster_spec.backup is not None:
        logs = os.path.join(cluster_spec.backup, 'logs')
        if os.path.exists(logs):
            shutil.make_archive('tools', 'zip', logs)


if __name__ == '__main__':
    main()
