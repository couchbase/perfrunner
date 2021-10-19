"""Fetch go dependencies by examining the manifest file.

This utility will:
- fetch the manifest file from one of host
- get the revision for required projects and packages
- get those dependencies using govendor
"""

import collections
import xml.etree.ElementTree
from argparse import ArgumentParser
from multiprocessing import set_start_method
from typing import Dict

from perfrunner.helpers.local import govendor_fetch
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec

set_start_method("fork")

REQUIRED_PROJECTS = ["indexing", "clog", "go-slab", "go_json",
                     "go-jsonpointer", "gojson"]
REQUIRED_PACKAGES = ["gometa", "query", "cbauth", "go-couchbase",
                     "gomemcached", "goutils"]


Project = collections.namedtuple('Project', 'name upstream revision path')


def parse_manifest() -> Dict[str, Project]:
    root = xml.etree.ElementTree.parse('manifest.xml').getroot()
    projects = {}
    for atype in root.findall('project'):
        name = atype.get('name')
        if name in REQUIRED_PROJECTS or name in REQUIRED_PACKAGES:
            upstream = atype.get('upstream')
            revision = atype.get('revision')
            path = atype.get('path').split("src/", 1)[1]
            project = Project(name, upstream, revision, path)
            projects[name] = project
    return projects


def fetch(projects: Dict[str, Project]):
    od = collections.OrderedDict(sorted(projects.items()))
    for name, project in od.items():
        if name in REQUIRED_PROJECTS:
            package_str = "^"
        else:
            package_str = "..."
        govendor_fetch(project.path, project.revision, package_str)


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a cluster specification file')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')

    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster)

    remote = RemoteHelper(cluster_spec, args.verbose)
    remote.get_manifest()
    projects = parse_manifest()
    fetch(projects)


if __name__ == '__main__':
    main()
