from .base import BaseEngine
from kqueen.config import current_config
from kqueen.server import app

import importlib
import json
import logging
import openstack
import os
import shutil
import subprocess
import sys
import time
import yaml

logger = logging.getLogger("kqueen_api")
config = current_config()

SSH_COMMON_ARGS = ("-o", "UserKnownHostsFile=/dev/null",
                   "-o", "StrictHostKeyChecking=no",
                   "-i", os.path.join(config.KS_FILES_PATH, "ssh_key"))


def get_template_and_files():
    # NOTE(sskripnick)
    # we need to custom load and decode all files because of this bug:
    # https://storyboard.openstack.org/#!/story/2002002
    package_name = "kqueen.engines.resources.heat.kubespray"
    package = importlib.import_module(package_name)
    path = list(package.__path__)[0]
    files = {}
    for root, dirs, _files in os.walk(path):
        for f in _files:
            fpath = os.path.join(root, f)
            with open(fpath, "rb") as f:
                files["file://" + fpath] = f.read().decode("utf8")
    return os.path.join(path, "template.yaml"), files


def generate_inventory(outputs, ssh_username):
    """Generate inventory object for kubespray.

    :param dict outputs: heat stack outputs

    Outputs may look like this:
    {
        "masters": [
            {"hostname": "host-1", "ip": "10.1.1.1"},
            {"hostname": "host-2", "ip": "10.1.1.2"},
            {"hostname": "host-3", "ip": "10.1.1.3"},
        ],
        "slaves": [
            {"hostname": "host-4", "ip": "10.1.1.4"},
            {"hostname": "host-5", "ip": "10.1.1.5"},
        ],
    }

    Return value is json serializable object to be used as kubespray
    inventory file.
    """
    ssh_common_args = " ".join(SSH_COMMON_ARGS)
    conf = {
        "all": {"hosts": {}},
        "kube-master": {
            "hosts": {},
            "vars": {
                "ansible_ssh_common_args": ssh_common_args
            },
        },
        "kube-node": {"hosts": {}},
        "etcd": {"hosts": {}},
        "vault": {"hosts": {}},
        "k8s-cluster": {"children": {"kube-node": None,
                                     "kube-master": None}},
    }
    for master in outputs["masters"]:
        conf["all"]["hosts"][master["hostname"]] = {
            "access_ip": master["ip"],
            "ansible_host": master["ip"],
            "ansible_user": ssh_username,
            "ansible_become": True,
        }
        conf["kube-master"]["hosts"][master["hostname"]] = None
        conf["etcd"]["hosts"][master["hostname"]] = None
        conf["vault"]["hosts"][master["hostname"]] = None
    for slave in outputs["slaves"]:
        conf["all"]["hosts"][slave["hostname"]] = {
            "ansible_host": slave["ip"],
            "ansible_user": ssh_username,
            "ansible_become": True,
        }
        conf["kube-node"]["hosts"][slave["hostname"]] = None
    ssh_args_fmt = ("-o ProxyCommand=\"ssh {}@{} "
                    "-o UserKnownHostsFile=/dev/null "
                    "-o StrictHostKeyChecking=no -W %h:%p\" " +
                    ssh_common_args)
    ssh_args = ssh_args_fmt.format(ssh_username, outputs["masters"][0]["ip"])
    conf["kube-node"]["vars"] = {"ansible_ssh_common_args": ssh_args}
    return conf


def get_ssh_key():
    """Generate ssh keypair if not exist.

    Return path to public key.
    """
    os.makedirs(config.KS_FILES_PATH, exist_ok=True)
    ssh_key = os.path.join(config.KS_FILES_PATH, "ssh_key")
    if not os.path.exists(ssh_key):
        subprocess.check_call([config.KS_SSH_KEYGEN_CMD, "-P", "", "-f", ssh_key])
    return os.path.join(config.KS_FILES_PATH, "ssh_key.pub")


class OpenStack:
    """Openstack client wrapper."""

    def __init__(self, connection):
        """Initialize Heat client wrapper.

        :param openstack.connection.Connection connection: openstacksdk
            connection instance
        """
        self.connection = connection

    def create_stack(self, *, stack_name, **kwargs):
        """Create Heat stack with given kwargs."""

        template_file, files = get_template_and_files()
        try:
            return self.connection.create_stack(
                name=stack_name,
                template_file=template_file,
                files=files,
                rollback=False,
                wait=True,
                **kwargs,
            )
        except openstack.exceptions.BadRequestException as e:
            # NOTE(sskripnick)
            # handle this exception separately because useful information
            # may be found only in e.details
            logger.exception("Failed to create stack: %s" % e.details)
            raise
        except Exception as e:
            logger.exception("Failed to create stack: %s" % e)
            raise

    def update_stack(self, stack_name, **kwargs):
        template_file, files = get_template_and_files()
        try:
            return self.connection.update_stack(
                name_or_id=stack_name,
                template_file=template_file,
                files=files,
                rollback=True,
                wait=True,
                **kwargs,
            )
        except openstack.exceptions.BadRequestException as e:
            # NOTE(sskripnick)
            # handle this exception separately because useful information
            # may be found only in e.details
            logger.exception("Failed to update stack: %s" % e.details)
            raise
        except Exception as e:
            logger.exception("Failed to update stack: %s" % e)
            raise

    def get_outputs(self, stack):
        outputs = {}
        for output in stack.outputs:
            outputs[output.output_key] = output.output_value
        return outputs

    def delete_stack(self, name):
        """Delete stack.

        :returns: True if delete succeeded, False if the stack was not found.
        """

        return self.connection.delete_stack(name)


class OpenstackKubesprayEngine(BaseEngine):
    """OpenStack Kubespray engine.

    This engine can provision k8s cluster in OpenStack cloud.

    Ansible and Kubespray should be installed and configured.

    Path to kubespray may be configured by setting KS_KUBESPRAY_PATH,
    e.g. by setting environment variable KQUEEN_KS_KUBESPRAY_PATH.

    Known to work with kubespray-2.5.0, ansible-2.4.3.0 and
    ubuntu 16.04 image.

    """
    name = "openstack_kubespray"
    verbose_name = "Openstack Kubespray Engine"
    parameter_schema = {
        "cluster": {
            "ssh_key_name": {
                "type": "text",
                "order": 10,
                "label": "SSH key name",
                "validators": {
                    "required": True,
                },
            },
            "ssh_username": {
                "type": "text",
                "order": 15,
                "label": "SSH username",
                "default": "ubuntu",
                "validators": {
                    "required": True,
                },
            },
            "image_name": {
                "type": "text",
                "order": 20,
                "label": "Image name",
                "validators": {
                    "required": True,
                },
            },
            "flavor": {
                "type": "text",
                "order": 30,
                "label": "Flavor",
                "validators": {
                    "required": True,
                },
            },
            "master_count": {
                "type": "integer",
                "order": 40,
                "default": 3,
                "label": "Master node count",
                "validators": {
                    "required": True,
                    "minimum": 3,
                },
            },
            "slave_count": {
                "type": "integer",
                "order": 50,
                "default": 1,
                "label": "Slave(minion) count",
                "validators": {
                    "required": True,
                },
            },
            "floating_network": {
                "order": 60,
                "type": "text",
                "label": "Floating network name or id",
                "default": "public",
            },
            "dns_nameservers": {
                "type": "text",
                "order": 70,
                "label": "Comma separated list of nameservers",
                "default": config.KS_DEFAULT_NAMESERVERS,
            },
            "availability_zone": {
                "type": "text",
                "order": 80,
                "label": "Availability zone",
                "default": "nova",
            },
        },
        "provisioner": {
            "auth_url": {
                "type": "text",
                "label": "Auth URL",
                "order": 10,
                "validators": {
                    "required": True,
                    "url": True,
                },
            },
            "username": {
                "type": "text",
                "label": "Username",
                "order": 20,
                "validators": {
                    "required": True,
                },
            },
            "password": {
                "type": "password",
                "label": "Password",
                "order": 30,
                "validators": {
                    "required": True,
                },
            },
            "domain_name": {
                "type": "text",
                "label": "Domain name",
                "order": 40,
                "validators": {
                    "required": True,
                },
            },
            "project_id": {
                "type": "text",
                "label": "Project ID",
                "order": 50,
                "validators": {
                    "required": True,
                },
            },
            "region_name": {
                "type": "text",
                "order": 60,
                "label": "Region name",
                "default": "RegionOne",
                "validators": {
                    "required": True,
                }
            },
            "identity_interface": {
                "type": "text",
                "label": "Identity interface",
                "order": 70,
                "default": "public",
                "validators": {
                    "required": True,
                }
            },
        }
    }

    def __init__(self, cluster, *args, **kwargs):
        super().__init__(cluster, *args, **kwargs)
        self.kwargs = kwargs
        self.stack_name = "kqueen-" + cluster.id
        self.os = OpenStack(openstack.connection.Connection(
            auth_url=kwargs["auth_url"],
            project_id=kwargs["project_id"],
            username=kwargs["username"],
            compute_api_version="3",
            domain_name=kwargs["domain_name"],
            identity_interface=kwargs["identity_interface"],
            password=kwargs["password"],
        ))

    def provision(self):
        try:
            self.cluster.state = config.CLUSTER_PROVISIONING_STATE
            self.cluster.save()
            app.executor.submit(self._run_provisioning)
        except Exception as e:
            logger.exception("Failed to submit provisioning task: %s" % e)
            self.cluster.state = config.CLUSTER_ERROR_STATE
            self.cluster.save()
            return False, e
        return True, None

    def _run_provisioning(self):
        # NOTE(sskripnick): This approach is not scalable.
        # It may be solved by storing ssh keys in etcd and
        # running ansible on the one of master nodes
        try:
            kwargs = {"ssh_key": open(get_ssh_key(), "r").read()}
            for key in ("ssh_key_name", "image_name", "flavor",
                        "master_count", "slave_count", "floating_network",
                        "dns_nameservers", "availability_zone"):
                kwargs[key] = self.kwargs[key]
            stack = self.os.create_stack(
                stack_name=self.stack_name,
                **kwargs,
            )
            outputs = self.os.get_outputs(stack)
            node_count = len(outputs["masters"]) + len(outputs["slaves"])
            self.cluster.metadata["node_count"] = node_count
            self.cluster.metadata["outputs"] = outputs
            self.cluster.metadata["kwargs"] = kwargs
            self.cluster.metadata["ssh_username"] = self.kwargs["ssh_username"]
            self.cluster.save()
            self._run_kubespray(outputs)
            self.cluster.state = config.CLUSTER_OK_STATE
            logger.info("Cluster provision completed")
        except Exception as e:
            self.cluster.state = config.CLUSTER_ERROR_STATE
            logger.exception("Failed to provision cluster: %s" % e)
        finally:
            self.cluster.save()

    def _run_kubespray(self, outputs, file="cluster.yml"):
        """Run kubespray."""
        inventory_path = self._generate_ansible_files(outputs)
        self._wait_for_ping(inventory_path)
        self._run_ansible(inventory_path, file)
        self._get_kubeconfig()

    def _get_ansible_files_path(self):
        return os.path.join(config.KS_FILES_PATH, self.cluster.id)

    def _generate_ansible_files(self, outputs):
        inventory = generate_inventory(outputs, self.kwargs["ssh_username"])
        path = self._get_ansible_files_path()
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "hosts.json"), "w") as hosts:
            json.dump(inventory, hosts)
        group_vars_path = os.path.join(path, "group_vars")
        if not os.path.isdir(group_vars_path):
            src = os.path.join(config.KS_KUBESPRAY_PATH,
                               "inventory/sample/group_vars")
            shutil.copytree(src, group_vars_path)
        all_yaml = open(os.path.join(path, "group_vars/all.yml"), "a")
        all_yaml.write("\ncloud_provider: openstack\n")
        all_yaml.close()
        return path

    def _wait_for_ping(self, path):
        retries = 10
        args = ["/usr/bin/ansible", "-m", "ping", "all", "-i", "hosts.json"]
        while retries:
            retries -= 1
            time.sleep(10)
            cp = subprocess.run(args, cwd=path)
            if cp.returncode == 0:
                return
        raise RuntimeError("At least one node is unreachable")

    def _construct_env(self):
        env = os.environ.copy()
        env.update({
            "OS_PROJECT_ID": self.kwargs["project_id"],
            "OS_TENANT_ID": self.kwargs["project_id"],
            "OS_REGION_NAME": self.kwargs["region_name"],
            "OS_USER_DOMAIN_NAME": self.kwargs["domain_name"],
            "OS_PROJECT_NAME": self.kwargs["project_id"],
            "OS_IDENTITY_API_VERSION": "3",
            "OS_PASSWORD": self.kwargs["password"],
            "OS_AUTH_URL": self.kwargs["auth_url"],
            "OS_USERNAME": self.kwargs["username"],
            "OS_INTERFACE": self.kwargs["identity_interface"],
        })
        return env

    def _run_ansible(self, path, file="cluster.yml"):
        hosts = os.path.join(path, "hosts.json")
        args = ["/usr/bin/ansible-playbook", "-b", "-i", hosts, file]
        env = self._construct_env()
        # TODO(sskripnick) Maybe collect out/err from pipe and log them
        # separately.
        pipe = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=sys.stdout,
            stderr=sys.stderr,
            cwd=config.KS_KUBESPRAY_PATH,
            env=env,
        )
        pipe.wait()
        if pipe.returncode:
            raise RuntimeError("Non zero exit status from ansible (%s)" % pipe.returncode)

    def _get_kubeconfig(self):
        cat_kubeconf = "sudo cat /etc/kubernetes/admin.conf"
        ip = self.cluster.metadata["outputs"]["masters"][0]["ip"]
        host = "@".join((self.cluster.metadata["ssh_username"], ip))
        args = ("ssh", host) + SSH_COMMON_ARGS + (cat_kubeconf,)
        kubeconfig = yaml.safe_load(subprocess.check_output(args))
        kubeconfig["clusters"][0]["cluster"]["server"] = "https://%s:6443" % ip
        self.cluster.kubeconfig = kubeconfig
        self.cluster.save()

    def deprovision(self):
        deleted = self.os.delete_stack(self.stack_name)
        if not deleted:
            logger.warn("deleting stack %s: not found" % self.stack_name)
        try:
            shutil.rmtree(self._get_ansible_files_path())
        except Exception as e:
            logger.warn("Unable to delete cluster files: %s" % e)
        return True, None

    def _scale_up(self, node_count):
        try:
            self.cluster.state = config.CLUSTER_RESIZING_STATE
            self.cluster.save()
            kwargs = self.cluster.metadata["kwargs"].copy()
            kwargs["slave_count"] = node_count
            stack = self.os.update_stack(self.stack_name, **kwargs)
            outputs = self.os.get_outputs(stack)
            node_count = len(outputs["masters"]) + len(outputs["slaves"])
            self.cluster.metadata["node_count"] = node_count
            self.cluster.metadata["outputs"] = outputs
            self.cluster.metadata["kwargs"] = kwargs
            self.cluster.state = config.CLUSTER_RESIZING_STATE
            self.cluster.save()
            self._run_kubespray(outputs, "scale.yml")
            self.cluster.state = config.CLUSTER_OK_STATE
            self.cluster.save()
            return True, "OK"
        except Exception as e:
            logger.exception("Failed to resize cluster: %s" % e)
            self.cluster.state = config.CLUSTER_ERROR_STATE
            self.cluster.save()

    def _scale_down(self, delta):
        try:
            self.cluster.state = config.CLUSTER_RESIZING_STATE
            self.cluster.save()
            slaves_number = len(self.cluster.metadata["outputs"]["slaves"])
            new_slaves_number = slaves_number - delta
            # TODO
            self.cluster.state = config.CLUSTER_OK_STATE
            self.cluster.save()
        except Exception as e:
            self.cluster.state = config.CLUSTER_ERROR_STATE
            self.cluster.save()
            logger.exception("Error resizing cluster: %s" % e)
            return False, "Exception: %s" % e
        self.cluster.state = config.CLUSTER_OK_STATE
        self.cluster.save()
        return False, "Not implemented"

    def resize(self, node_count):
        logger.info("Resize to %s nodes requested" % node_count)
        master_count = self.cluster.metadata["kwargs"]["master_count"]
        # NOTE(sskripnick) kqueen-ui sends node_count as string
        new_slave_count = int(node_count) - master_count
        delta = new_slave_count - self.cluster.metadata["kwargs"]["slave_count"]
        if -delta < master_count:
            return False, "Node count should be at least %s" % master_count
        if delta > 0:
            app.executor.submit(self._scale_up, new_slave_count)
            return True, "Resizing started"
        elif delta < 0:
            app.executor.submit(self._scale_down, -delta)
            return True, "Resizing started"
        return False, "Cluster has already %s nodes" % node_count

    def get_kubeconfig(self):
        return self.cluster.kubeconfig

    def cluster_get(self):
        return {
            "key": self.stack_name,       # (str) this record should be cached under this key if you choose to cache
            "name": self.stack_name,      # (str) name of the cluster in its respective backend
            "id": self.cluster.id,        # (str or UUID) id of `kqueen.models.Cluster` object in KQueen database
            "state": self.cluster.state,  # (str) state of cluster on backend represented by app.config["CLUSTER_[FOO]_STATE"]
            "metadata": {},               # any keys specific for the Provisioner implementation
        }

    def cluster_list(self):
        return []

    @classmethod
    def engine_status(cls, **kwargs):
        try:
            for cmd in ("KS_SSH_KEYGEN_CMD", "KS_SSH_CMD", "KS_ANSIBLE_CMD"):
                if not os.access(config.get(cmd), os.X_OK):
                    raise RuntimeError("%s is not properly configured" % cmd)
            heat = OpenStack(openstack.connection.Connection(**kwargs))
            list(heat.connection.orchestration.stacks(limit=1))
            return config.PROVISIONER_OK_STATE
        except Exception as e:
            logging.exception("Error engine status: %s", e)
            return config.PROVISIONER_ERROR_STATE
