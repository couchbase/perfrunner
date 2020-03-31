import pkg_resources
from logger import logger

cb_version = pkg_resources.get_distribution("couchbase").version
logger.info("CB Python SDK Version: "+str(cb_version))

if cb_version[0] == '2':
    from spring.wgen import WorkloadGen
elif cb_version[0] == '3':
    if cb_version == '3.0.0b3':
        from spring.wgen import WorkloadGen
    else:
        from spring.wgen3 import WorkloadGen


def spring_workload(*args):
    wg = WorkloadGen(*args)
    wg.run()
