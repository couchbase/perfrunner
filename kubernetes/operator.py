from argparse import ArgumentParser
import os
import subprocess
import fileinput
import time


class OperatorManager:

    def __init__(self, options):
        self.options = options
        if self.options.kube_config:
            self.kube_config_path = self.options.kube_config
        else:
            self.kube_config_path = os.path.expanduser("~") + "/.kube/config"
        if self.options.install or self.options.uninstall:
            self.release = self.options.operator_version.split("-")[0]
            self.build = self.options.operator_version.split("-")[1]

    def kubectl_get(self, type, extra=None):
        if extra is not None:
            params = ['kubectl', 'get', type, '--kubeconfig', self.kube_config_path] + extra
            ret = subprocess.check_output(params)
        else:
            ret = subprocess.check_output(['kubectl', 'get', type,
                                        '--kubeconfig', self.kube_config_path])
        return ret.splitlines()

    def kubectl_create(self, file_path):
        print(subprocess.check_output(['kubectl', 'create', '-f', file_path,
                                 '--kubeconfig', self.kube_config_path]))

    def kubectl_create_docker_secret(self):
        path = os.path.expanduser("~") + "/.docker/config.json"
        print(subprocess.check_output(['kubectl', 'create', 'secret', 'generic', 'regcred',
                                       '--from-file=.dockerconfigjson='+path, '--type=kubernetes.io/dockerconfigjson',
                                       '--kubeconfig', self.kube_config_path]))

    def kubectl_create_operator_tls_secret(self):
        path = os.path.join(os.path.join(os.path.dirname(__file__),
                                         "resources", self.release, "ca.crt"))
        print(subprocess.check_output(['kubectl', 'create', 'secret', 'generic', 'couchbase-operator-tls',
                                       '--from-file', path,
                                       '--kubeconfig', self.kube_config_path]))

    def kubectl_create_server_tls_secret(self):
        path1 = os.path.join(os.path.join(os.path.dirname(__file__),
                                          "resources", self.release, "chain.pem"))
        path2 = os.path.join(os.path.join(os.path.dirname(__file__),
                                          "resources", self.release, "pkey.key"))

        print(subprocess.check_output(['kubectl', 'create', 'secret', 'generic', 'couchbase-server-tls',
                                       '--from-file', path1, '--from-file', path2,
                                       '--kubeconfig', self.kube_config_path]))

    def kubectl_delete_server_tls_secret(self):
        try:
            print(subprocess.check_output(['kubectl', 'delete', 'secret', 'generic', 'couchbase-server-tls',
                                       '--kubeconfig', self.kube_config_path]))
        except Exception as e:
            print(str(e))

    def kubectl_delete_operator_tls_secret(self):
        try:
            print(subprocess.check_output(['kubectl', 'delete', 'secret', 'generic', 'couchbase-operator-tls',
                                           '--kubeconfig', self.kube_config_path]))
        except Exception as e:
            print(str(e))

    def kubectl_delete_docker_secret(self):
        try:
            print(subprocess.check_output(['kubectl', 'delete', 'secret', 'regcred',
                                        '--kubeconfig', self.kube_config_path]))
        except Exception as e:
            print(str(e))

    def kubectl_delete(self, file_path):
        try:
            print(subprocess.check_output(['kubectl', 'delete', '-f', file_path,
                                    '--kubeconfig', self.kube_config_path]))
        except Exception as e:
            print(str(e))

    def wait_for_admission_controller(self):
        pod_rdy = False
        i = 0
        while not pod_rdy and i < 300:
            pods = self.kubectl_get("pods")
            for line in pods:
                split_line = str(line).split()
                if "couchbase-operator-admission" in split_line[0]:
                    ready = split_line[1]
                    rdy = ready.split("/")[0]
                    total = ready.split("/")[1]
                    if rdy == total:
                        pod_rdy = True
            i += 1
            time.sleep(1)
        if not pod_rdy:
            raise Exception('admission controller not ready')

    def wait_for_operator(self):
        pod_rdy = False
        i = 0
        while not pod_rdy and i < 300:
            pods = self.kubectl_get("pods")
            for line in pods:
                split_line = str(line).split()
                if "couchbase-operator-admission" not in split_line[0] and \
                        "couchbase-operator" in split_line[0]:
                    ready = split_line[1]
                    rdy = ready.split("/")[0]
                    total = ready.split("/")[1]
                    if rdy == total:
                        pod_rdy = True
            i += 1
            time.sleep(1)
        if not pod_rdy:
            raise Exception("operator not ready")

    def wait_for_cluster(self):
        all_pods_rdy = False
        i = 0
        while not all_pods_rdy and i < 300:
            pods = self.kubectl_get("pods")
            num_rdy = 0
            for line in pods:
                split_line = str(line).split()
                if "cb-example" in split_line[0]:
                    ready = split_line[1]
                    rdy = ready.split("/")[0]
                    total = ready.split("/")[1]
                    if rdy == total:
                        num_rdy += 1
            if num_rdy == 3:
                all_pods_rdy = True
            else:
                i += 1
                time.sleep(1)
        if not all_pods_rdy:
            raise Exception("operator not ready")

    def wait_for_cluster_delete(self):
        cluster_deleted = False
        i = 0
        while not cluster_deleted and i < 300:
            pods = self.kubectl_get("pods")
            remaining = 0
            for line in pods:
                split_line = str(line).split()
                if "cb-example" in split_line[0]:
                    remaining += 1
            if remaining == 0:
                cluster_deleted = True
            else:
                i += 1
                time.sleep(1)
        if not cluster_deleted:
            raise Exception("operator not ready")

    def wait_for_operator_and_admission_delete(self):
        deleted = False
        i = 0
        while not deleted and i < 300:
            pods = self.kubectl_get("pods")
            remaining = 0
            for line in pods:
                split_line = str(line).split()
                if "couchbase-operator" in split_line[0]:
                    remaining += 1
            if remaining == 0:
                deleted = True
            else:
                i += 1
                time.sleep(1)
        if not deleted:
            raise Exception("operator not ready")

    def get_connect_ip_and_port(self):
        nodes = self.kubectl_get("nodes", ["-o", "wide"])
        found_ip = False
        for line in nodes:
            print(str(line))
            if "master" in str(line) and "Ready" in str(line):
                ip = str(line).split()[5]
                found_ip = True
        if not found_ip:
            raise Exception("could not get node ip")

        node_ports = nodes = self.kubectl_get("svc", ["cb-example-ui"])
        found_port = False
        for line in node_ports:
            if "cb-example-ui" in str(line) and "NodePort" in str(line):
                ports = str(line).split()[4]
                for config in ports.split(","):
                    for item in config.split("/"):
                        if '8091' in item.split(":"):
                            mapped_ports = item.split(":")
                            mapped_ports.remove("8091")
                            port = mapped_ports[0]
                            found_port = True
        if not found_port:
            raise Exception("could not get node port")
        return [ip, port]

    def install(self):
        if self.release == '2.0.0':
            self.install200()
        elif self.release == '1.2.0':
            self.install120()
        else:
            print("nothing to install")

    def install120(self):
        self.kubectl_create_docker_secret()

        self.kubectl_create_operator_tls_secret()

        self.kubectl_create_server_tls_secret()

        admission_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                "resources", self.release, "admission.yaml"))

        with fileinput.FileInput(admission_path, inplace=True, backup='.bak') as file:
            search = 'couchbase/admission-controller:'+self.release
            replace = 'couchbase/couchbase-admission-internal:'+self.release+'-'+self.build
            for line in file:
                print(line.replace(search, replace), end='')

        self.kubectl_create(admission_path)

        print("waiting for admission controller to be up and running")
        self.wait_for_admission_controller()

        crd_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                             "resources", self.release, "crd.yaml"))
        self.kubectl_create(crd_path)

        op_rol_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                             "resources", self.release, "operator-role.yaml"))
        self.kubectl_create(op_rol_path)

        op_service_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                "resources", self.release, "operator-service-account.yaml"))
        self.kubectl_create(op_service_path)

        op_role_binding_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                    "resources", self.release, "operator-role-binding.yaml"))
        self.kubectl_create(op_role_binding_path)

        op_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                "resources", self.release, "operator-deployment.yaml"))
        with fileinput.FileInput(op_path, inplace=True, backup='.bak') as file:
            search = 'couchbase/operator:'+self.release
            replace = 'couchbase/couchbase-operator-internal:'+self.release+'-'+self.build
            for line in file:
                print(line.replace(search, replace), end='')

        self.kubectl_create(op_path)

        print("waiting for operator to be up and running")
        self.wait_for_operator()

        secret_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                              "resources", self.release, "secret.yaml"))
        self.kubectl_create(secret_path)

        cluster_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                 "resources", self.release, "couchbase-cluster.yaml"))
        if self.options.couchbase_version:
            with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
                search = 'couchbase/server'
                for line in file:
                    if search in line:
                        line = '  image: couchbase/server:'+self.options.couchbase_version+'\n'
                    print(line, end='')

        self.kubectl_create(cluster_path)

        print("waiting for cluster to be up and running")
        self.wait_for_cluster()

    def install200(self):

        # create docker secret... this require you to docker login before running script

        self.kubectl_create_docker_secret()

        self.kubectl_create_operator_tls_secret()

        self.kubectl_create_server_tls_secret()

        crd_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                             "resources", self.release, "crd.yaml"))
        self.kubectl_create(crd_path)

        config_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                             "resources", self.release, "config.yaml"))
        with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
            search = 'couchbase/operator:'+self.release
            replace = 'couchbase/couchbase-operator-internal:'+self.release+'-'+self.build
            for line in file:
                print(line.replace(search, replace), end='')

        with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
            search = 'couchbase/admission-controller:'+self.release
            replace = 'couchbase/couchbase-admission-internal:'+self.release+'-'+self.build
            for line in file:
                print(line.replace(search, replace), end='')

        self.kubectl_create(config_path)

        print("waiting for admission controller to be up and running")
        self.wait_for_admission_controller()
        print("waiting for operator to be up and running")
        self.wait_for_operator()

        auth_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                 "resources", self.release, "auth_secret.yaml"))
        self.kubectl_create(auth_path)

        bucket_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                 "resources", self.release, "bucket.yaml"))
        self.kubectl_create(bucket_path)

        cluster_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                 "resources", self.release, "couchbase-cluster.yaml"))
        if self.options.couchbase_version:
            with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
                search = 'couchbase/server'
                for line in file:
                    if search in line:
                        line = '  image: couchbase/server:'+self.options.couchbase_version+'\n'
                    print(line, end='')

        self.kubectl_create(cluster_path)

        print("waiting for cluster to be up and running")
        self.wait_for_cluster()

    def delete_couchbase_cluster(self):
        try:
            print(subprocess.check_output(['kubectl', 'delete', 'couchbasecluster', 'cb-example',
                                           '--kubeconfig', self.kube_config_path]))
        except Exception as e:
            print(str(e))

    def uninstall(self):
        if self.release == '2.0.0':
            self.uninstall200()
        elif self.release == '1.2.0':
            self.uninstall120()
        else:
            print("nothing to uninstall")

    def uninstall120(self):
        cluster_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                 "resources", self.release, "couchbase-cluster.yaml"))
        self.kubectl_delete(cluster_path)

        self.wait_for_cluster_delete()

        secret_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                "resources", self.release, "secret.yaml"))
        self.kubectl_delete(secret_path)

        op_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                            "resources", self.release, "operator-deployment.yaml"))
        self.kubectl_delete(op_path)

        op_rol_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                "resources", self.release, "operator-role.yaml"))
        self.kubectl_delete(op_rol_path)

        op_service_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                    "resources", self.release, "operator-service-account.yaml"))
        self.kubectl_delete(op_service_path)

        op_role_binding_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                         "resources", self.release, "operator-role-binding.yaml"))
        self.kubectl_delete(op_role_binding_path)

        admission_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                   "resources", self.release, "admission.yaml"))

        self.kubectl_delete(admission_path)

        self.wait_for_operator_and_admission_delete()

        crd_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                             "resources", self.release, "crd.yaml"))
        self.kubectl_delete(crd_path)

        self.kubectl_delete_docker_secret()

        self.kubectl_delete_operator_tls_secret()

        self.kubectl_delete_server_tls_secret()

    def uninstall200(self):

        bucket_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                 "resources", self.release, "bucket.yaml"))
        self.kubectl_delete(bucket_path)

        cluster_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                 "resources", self.release, "couchbase-cluster.yaml"))
        self.kubectl_delete(cluster_path)

        self.wait_for_cluster_delete()

        auth_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                "resources", self.release, "auth_secret.yaml"))
        self.kubectl_delete(auth_path)

        config_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                                "resources", self.release, "config.yaml"))
        self.kubectl_delete(config_path)

        self.wait_for_operator_and_admission_delete()

        crd_path = os.path.join(os.path.join(os.path.dirname(__file__),
                                             "resources", self.release, "crd.yaml"))
        self.kubectl_delete(crd_path)

        self.kubectl_delete_docker_secret()

        self.kubectl_delete_operator_tls_secret()

        self.kubectl_delete_server_tls_secret()


def get_args():

    parser = ArgumentParser()
    parser.add_argument('--install',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('--uninstall',
                        action='store_true',
                        help='save a local copy of a package')
    parser.add_argument('--test',
                        action='store_true',
                        help='save a local copy of a package')
    parser.add_argument('-ov', '--operator-version',
                        help='the build version or the HTTP URL to a package')
    parser.add_argument('-cv', '--couchbase-version',
                        help='the path to a cluster specification file')
    parser.add_argument('-kc', '--kube-config',
                        help='the build version or the HTTP URL to a package')

    return parser.parse_args()


def main():
    args = get_args()

    operatorManager = OperatorManager(args)
    if args.install:
        operatorManager.install()
    elif args.uninstall:
        operatorManager.uninstall()
    elif args.test:
        "Not implemented yet"

    else:
        "Not a valid selection"

if __name__ == '__main__':
    main()