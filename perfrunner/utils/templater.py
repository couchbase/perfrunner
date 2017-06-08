from argparse import ArgumentParser

from jinja2 import Environment, FileSystemLoader, Template


MEMORY_QUOTAS = {
    'c3.4xlarge': 24576,  # 30GB RAM
    'm4.2xlarge': 26624,  # 32GB RAM
}

OUTPUT_FILE = 'custom.test'

TEMPLATES_DIR = 'templates'

TEMPLATES = {
    'pillowfight': 'pillowfight.j2',
    'ycsb_workload_a': 'ycsb_workload_a.j2',
}

THREADS_PER_CLIENT = {
    'pillowfight': 20,
    'ycsb_workload_a': 20,
}


def render(template: str, instance: str, threads: int):
    loader = FileSystemLoader(searchpath=TEMPLATES_DIR)
    env = Environment(loader=loader, keep_trailing_newline=True)

    t = env.get_template(TEMPLATES[template])

    mem_quota = MEMORY_QUOTAS[instance]
    worker_instances = estimate_num_clients(template, threads)

    content = render_template(t, mem_quota, worker_instances)
    store_cfg(content)


def estimate_num_clients(template: str, threads: int) -> int:
    return max(1, threads // THREADS_PER_CLIENT[template])


def render_template(t: Template, mem_quota: int, worker_instances: int) -> str:
    return t.render(
        mem_quota=mem_quota,
        worker_instances=worker_instances,
    )


def store_cfg(content: str):
    with open(OUTPUT_FILE, 'w') as f:
        f.write(content)


def main():
    parser = ArgumentParser()

    parser.add_argument('--instance', dest='instance', type=str,
                        choices=list(MEMORY_QUOTAS),
                        help='EC2 instance type')
    parser.add_argument('--template', dest='template', type=str,
                        choices=list(TEMPLATES),
                        help='Template name')
    parser.add_argument('--threads', dest='threads', type=int,
                        default=1,
                        help='Total number of workload generator threads')

    args = parser.parse_args()

    render(args.template, args.instance, args.threads)


if __name__ == '__main__':
    main()
