import glob
import os.path
import shutil
from optparse import OptionParser

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

    remote.collect_info()
    for hostname in cluster_spec.hostnames:
        for fname in glob.glob('{}/*.zip'.format(hostname)):
            shutil.move(fname, '{}.zip'.format(hostname))

    if cluster_spec.backup is not None:
        logs = os.path.join(cluster_spec.backup, 'logs')
        if os.path.exists(logs):
            shutil.make_archive('tools', 'zip', logs)


if __name__ == '__main__':
    main()
