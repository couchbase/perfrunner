from spring.wgen import WorkloadGen


def spring_workload(*args, **kwargs):
    wg = WorkloadGen(*args, **kwargs)
    wg.run()
