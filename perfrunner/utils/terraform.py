#!/usr/bin/env python
import configparser
import os
from argparse import ArgumentParser
from uuid import uuid4

from perfrunner.settings import ClusterSpec


class Terraform:
    # TODO: Multiple region support,
    #    AWS EBS volumes,
    #    multiple instance types in one role,
    #    capacity retry for AWS.

    worker_home = os.getcwd()

    services = []

    def __init__(self, infra_spec, options):
        self.worker_home = os.getcwd()
        self.services = []
        self.suffix = uuid4().hex[0:6]
        self.infra_spec = infra_spec
        self.options = options
        self.backup_url = ""
        self.parser = configparser.ConfigParser()
        self.parser.read(self.options.cluster)
        self.node_config = self.node_config_generate()
        self.run()

    def run(self):
        self.initialize_terraform()
        self.is_backup()
        self.inject_nodes_terraform()
        self.terraform_apply_and_output()
        self.update_spec()
        if self.infra_spec.cloud_provider in ['azure', 'gcp']:
            self.append_pips()
        if self.infra_spec.infrastructure_settings.get('backup', 'false') == "true":
            self.append_backup_url()
        self.destroy()

    # Runs terraform destroy on deployed infrastructure.
    def destroy(self):
        os.system('cd terraform/' + self.infra_spec.cloud_provider +
                  " && terraform destroy --auto-approve")
        os.system('cd ' + self.worker_home)

    # Initializes terraform environment.
    def initialize_terraform(self):
        os.system('cd terraform/' + self.infra_spec.cloud_provider + ' && terraform init')
        os.system('cd ' + self.worker_home)

    # Checks if backup storage is required, populates storage resource if so.
    def is_backup(self):
        if self.infra_spec.infrastructure_settings.get('backup', 'false') == "true":
            self.find_and_replace("BACKUP", "\"1\"")
            self.backup_url = "az://{}".format("content" + self.suffix)
        else:
            self.find_and_replace("BACKUP", "\"0\"")

    # Find and replace in respective tf.
    def find_and_replace(self, placeholder, value):
        with open("terraform/" + self.infra_spec.cloud_provider + '/Terraform.tf', 'r') as file:
            data = file.read()
        data = data.replace(placeholder, value)
        with open("terraform/" + self.infra_spec.cloud_provider + '/Terraform.tf', 'w') as file:
            file.write(data)

    # Creates entries for cluster parameters, replaces corresponding placeholder in tf file.
    def populate_tf(self, key):
        placeholder = self.node_config[key]['role'].upper()
        instance = '\"' + self.node_config[key]['instance'] + '\"'
        capacity = '\"' + self.node_config[key]['capacity'] + '\"'
        disk = '\"' + self.node_config[key]['disk'] + '\"'
        d_type = '\"' + self.node_config[key]['d_type'] + '\"'
        self.find_and_replace("SUFFIX", self.suffix)
        self.find_and_replace("STORAGE_TYPE", d_type)
        self.find_and_replace(placeholder + "_INSTANCE", instance)
        self.find_and_replace(placeholder + "_CAPACITY", capacity)
        self.find_and_replace(placeholder + "_DISK", disk)

    # Appends node group resources to terraform file.
    def inject_nodes_terraform(self):
        for key in self.node_config:
            self.populate_tf(key)

    # Parse node groups and their params from spec file.
    def get_node_groups(self):
        node_group_int = 1
        node_groups = {}
        storage_type = self.parser['ec2_cluster_1']
        d_type = storage_type['storage_class']
        while True:
            key = "ec2_node_group_" + str(node_group_int)
            if key in self.parser:
                options = self.parser[key]
                group = {'instance': options['instance_type'],
                         'capacity': options['instance_capacity'],
                         'nodes': [], 'role': "",
                         'disk': options['volume_size'],
                         'd_type': d_type}
                node_groups["node_group_" + str(node_group_int)] = group
                node_group_int += 1
            else:
                break
        return node_groups

    # List all nodes used in the test, pop empty val from server and client groups.
    def get_nodes(self, role, nodes):
        for val in nodes:
            node_list = nodes[val].split("\n")
        if role != 'utilities':
            node_list.pop(0)
        return node_list

    # Generates a dictionary where keys = node groups, values = dictionary of node group params.
    # i.e. {node_group_1 : {'instance': 't3.micro', 'capacity': '3','nodes': [node1, node2],
    # 'role': "cluster", disk': 40}
    def node_config_generate(self):
        terraform_settings = {'cluster': self.get_nodes('cluster',
                                                        self.infra_spec.infrastructure_clusters),
                              'clients': self.get_nodes('clients',
                                                        self.infra_spec.infrastructure_clients),
                              'utilities': self.get_nodes('utilities',
                                                          self.infra_spec.infrastructure_utilities)}
        node_groups = self.get_node_groups()
        for key in node_groups:
            if key in "".join(terraform_settings['cluster']):
                node_groups[key]['role'] = 'cluster'
                for val in terraform_settings['cluster']:
                    self.services.append(val.split(":", 1)[1])
                    node_groups[key]['nodes'].append(val)
            elif key in "".join(terraform_settings['clients']):
                node_groups[key]['role'] = 'clients'
                for val in terraform_settings['clients']:
                    node_groups[key]['nodes'].append(val)
            elif key in "".join(terraform_settings['utilities']):
                node_groups[key]['role'] = 'utilities'
                for val in terraform_settings['utilities']:
                    if key in val:
                        node_groups[key]['nodes'].append(val)
            else:
                print('Error no node group for node.')
        return node_groups

    # Apply Terraform plan, pipe ip output to dns.py.
    def terraform_apply_and_output(self):
        os.system("cd terraform/" +
                  self.infra_spec.cloud_provider + " && "
                                                   "terraform apply --auto-approve && "
                                                   "terraform output > "
                                                   "" + self.worker_home + "/terraform/ip.py")
        os.system('cd ' + self.worker_home)

    # Update spec file with ip values.
    def update_spec(self):
        import perfrunner.terraform.out as ip
        cluster_ips, client_ips, utility_ips = "", "", ""
        service_counter = 0
        for val in ip.cluster_public_ip:
            cluster_ips += "\n" + val + ":" + self.services[service_counter]
            service_counter += 1
        self.parser.set('clusters', 'couchbase1', cluster_ips)
        for val in ip.clients_public_ip:
            client_ips += "\n" + val
        self.parser.set('clients', 'workers1', client_ips)
        for val in ip.utilities_public_ip:
            utility_ips += "\n" + val
        self.parser.set('utilities', 'brokers1', utility_ips)
        with open(self.options.cluster, 'w') as spec:
            self.parser.write(spec)
        with open('cloud/infrastructure/cloud.ini') as f:
            s = f.read()
        with open('cloud/infrastructure/cloud.ini', 'w') as f:
            s = s.replace("server_list", "\n".join(ip.cluster_public_ip))
            s = s.replace("worker_list", "\n".join(ip.clients_public_ip))
            f.write(s)

    # Append private ips to spec.
    def append_pips(self):
        import terraform.ip as ip
        pips = ""
        for val in ip.cluster_private_ip:
            pips += "\n" + val
        self.parser.add_section('private_ips')
        self.parser.set('private_ips',
                        list(self.infra_spec.infrastructure_clusters.keys())[0],
                        pips)
        with open(self.options.cluster, 'w') as spec:
            self.parser.write(spec)

    def append_backup_url(self):
        self.parser.set('storage', 'backup', self.backup_url)
        with open(self.options.cluster, 'w') as spec:
            self.parser.write(spec)


# CLI args.
def get_args():
    parse = ArgumentParser()

    parse.add_argument('-c', '--cluster',
                       required=True,
                       help='the path to a infrastructure specification file')
    parse.add_argument('--verbose',
                       action='store_true',
                       help='enable verbose logging')
    parse.add_argument('-r', '--region',
                       choices=['us-east-1', 'us-west-2'],
                       default='us-east-1',
                       help='the cloud region (AWS)')
    parse.add_argument('-z', '--zone',
                       choices=[
                           'us-central1-a',
                           'us-central1-b',
                           'us-central1-c'
                           'us-central1-f',
                           'us-west1-a',
                           'us-west1-b',
                           'us-west1-c'
                       ],
                       default='us-west1-b',
                       help='the cloud zone (GCP)')
    parse.add_argument('-t', '--tag',
                       default='<None>',
                       help='Global tag for launched instances.')

    return parse.parse_args()


def main():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster)
    Terraform(infra_spec, args)
