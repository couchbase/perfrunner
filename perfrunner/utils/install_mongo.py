from optparse import OptionParser

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec
from perfrunner.utils.install import CouchbaseInstaller


class MongoDBInstaller(CouchbaseInstaller):

    URL = 'http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.6.1.tgz'

    def __init__(self, cluster_spec, options):
        self.remote = RemoteHelper(cluster_spec, None, options.verbose)

    def uninstall_package(self):
        pass

    def clean_data(self):
        self.remote.clean_mongodb()

    def install_package(self):
        self.remote.install_mongodb(url=self.URL)


def main():
    usage = '%prog -c cluster.spec'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')

    options, args = parser.parse_args()

    if not options.cluster_spec_fname:
        parser.error('Missing mandatory parameter')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, args)

    installer = MongoDBInstaller(cluster_spec, options)
    installer.install()

if __name__ == '__main__':
    main()
