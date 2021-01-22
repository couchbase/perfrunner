import pkg_resources
from logger import logger


def spring_workload(*args):
    cb_version = pkg_resources.get_distribution("couchbase").version
    logger.info("CB Python SDK Version: "+str(cb_version))
    if cb_version[0] == '2':
        from spring.wgen import WorkloadGen
        wg = WorkloadGen(*args)
        wg.run()
    elif cb_version[0] == '3':
        from spring.wgen3 import WorkloadGen
        wg = WorkloadGen(*args)
        wg.run()
