from argparse import ArgumentParser

import yaml
from jinja2 import Environment, FileSystemLoader, Template

from logger import logger
from perfrunner.utils.cloudrunner import CloudRunner

MEMORY_QUOTAS = {
    'm4.2xlarge': 26624,  # 32GB RAM
    'm4.4xlarge': 56320,  # 64GB RAM
}

OUTPUT_FILE = 'custom'

TEMPLATES_DIR = 'templates'

TEMPLATES = (
    'full_cluster.spec',
    'kv_cluster.spec',
    'pillowfight.test',
    'ycsb_workload_a.test',
    'ycsb_workload_e.test',
)

THREADS_PER_CLIENT = {
    'pillowfight.test': 20,
    'ycsb_workload_a.test': 20,
    'ycsb_workload_e.test': 20,
}


def get_templates(template: str) -> Template:
    loader = FileSystemLoader(searchpath=TEMPLATES_DIR)
    env = Environment(loader=loader, keep_trailing_newline=True)
    return env.get_template(template)


def render_test(template: str, instance: str, threads: int):
    mem_quota = MEMORY_QUOTAS[instance]
    worker_instances = estimate_num_clients(template, threads)
    content = render_template(get_templates(template),
                              mem_quota=mem_quota,
                              worker_instances=worker_instances)
    store_cfg(content, '.test')


def render_spec(template: str):
    with open(CloudRunner.EC2_META) as fp:
        meta = yaml.load(fp)
        clients = meta.get('clients', {}).values()
        servers = meta.get('servers', {}).values()

    content = render_template(get_templates(template),
                              servers=servers,
                              clients=clients)
    store_cfg(content, '.spec')


def render_inventory():
    with open(CloudRunner.EC2_META) as fp:
        meta = yaml.load(fp)
        servers = meta.get('servers', {}).values()

    content = render_template(get_templates('inventory.ini'),
                              servers=servers)
    store_cfg(content, '.ini')


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

    parser.add_argument('--instance', dest='instance', type=str,
                        choices=list(MEMORY_QUOTAS))
    parser.add_argument('--template', dest='template', type=str,
                        choices=TEMPLATES,
                        required=True)
    parser.add_argument('--threads', dest='threads', type=int,
                        default=1,
                        help='Total number of workload generator threads')

    args = parser.parse_args()

    if '.test' in args.template:
        render_test(args.template, args.instance, args.threads)
    else:
        render_spec(args.template)
        render_inventory()


if __name__ == '__main__':
    main()
