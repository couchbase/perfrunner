import csv
from argparse import ArgumentParser

from jinja2 import Environment, FileSystemLoader, Template
from logger import logger

MEMORY_QUOTAS = {
    'c3.4xlarge': 24576,  # 30GB RAM
    'm4.2xlarge': 26624,  # 32GB RAM
}

OUTPUT_FILE = 'custom'

TEMPLATES_DIR = 'templates'

TEMPLATES = {
    'pillowfight': 'pillowfight.j2',
    'ycsb_workload_a': 'ycsb_workload_a.j2',
    'cluster_spec': 'cluster.j2'
}

THREADS_PER_CLIENT = {
    'pillowfight': 20,
    'ycsb_workload_a': 20,
}

TEMPLATE_TYPE = 'cluster', 'test'


def get_templates(template: str) -> Template:
    loader = FileSystemLoader(searchpath=TEMPLATES_DIR)
    env = Environment(loader=loader, keep_trailing_newline=True)
    return env.get_template(TEMPLATES[template])


def render_test(template: str, instance: str, threads: int):
    mem_quota = MEMORY_QUOTAS[instance]
    worker_instances = estimate_num_clients(template, threads)
    content = render_template(get_templates(template),
                              mem_quota=mem_quota,
                              worker_instances=worker_instances)
    store_cfg(content, '.test')


def render_spec(template: str, spec_file: str, key: str):
    server = []
    client = []
    with open(spec_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['type'] == 'client':
                client.append(row['private_ip'])
            else:
                server.append(row['private_ip'])

    content = render_template(get_templates(template),
                              servers=server,
                              clients=client,
                              key=key)
    store_cfg(content, '.spec')


def estimate_num_clients(template: str, threads: int) -> int:
    return max(1, threads // THREADS_PER_CLIENT[template])


def render_template(t: Template, **kwargs) -> str:
    return t.render(**kwargs)


def store_cfg(content: str, extension: str):
    logger.info('Creating a new file: {}'.format(OUTPUT_FILE + extension))
    with open(OUTPUT_FILE + extension, 'w') as f:
        f.write(content)


def main():
    parser = ArgumentParser()

    parser.add_argument('--type', dest='type', type=str,
                        choices=list(TEMPLATE_TYPE),
                        required=True,
                        help='Type of template')
    parser.add_argument('--instance', dest='instance', type=str,
                        choices=list(MEMORY_QUOTAS),
                        help='EC2 instance type')
    parser.add_argument('--template', dest='template', type=str,
                        choices=list(TEMPLATES),
                        required=True,
                        help='Template name')
    parser.add_argument('--threads', dest='threads', type=int,
                        default=1,
                        help='Total number of workload generator threads')
    parser.add_argument('--spec', dest='spec', type=str,
                        help='Spec file required for cloudrunner')
    parser.add_argument('--key', dest='key', type=str,
                        help='key file for login')

    args = parser.parse_args()

    if args.type == 'test':
        render_test(args.template, args.instance, args.threads)
    else:
        render_spec(args.template, args.spec, args.key)


if __name__ == '__main__':
    main()
