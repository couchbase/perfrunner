import importlib.metadata

from logger import logger


def spring_workload(*args):
    sdk_version = importlib.metadata.version("couchbase")
    logger.info("CB Python SDK Version: " + sdk_version)
    sdk_major_version = int(sdk_version[0])
    if sdk_major_version == 2:
        from spring.wgen import WorkloadGen
        wg = WorkloadGen(*args)
        wg.run()
    elif sdk_major_version >= 3:
        from spring.wgen3 import WorkloadGen
        wg = WorkloadGen(*args)
        wg.run()
