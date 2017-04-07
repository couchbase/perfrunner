from spring.wgen import WorkloadGen


def spring_workload(*args):
    wg = WorkloadGen(*args)
    wg.run()
