# -*- coding: utf-8 -*-
import datetime
import json
import math
import random
import re
import time
from typing import Any, Dict, List

from chaoslib.exceptions import ActivityFailed
from chaoslib.types import Secrets
from kubernetes import client
from kubernetes import stream
from kubernetes.stream.ws_client import ERROR_CHANNEL
from kubernetes.stream.ws_client import STDOUT_CHANNEL
from kubernetes.client.models.v1_pod import V1Pod
from logzero import logger

from chaosk8s import create_k8s_api_client

__all__ = ["kill_main_process", "terminate_pods", "exec_in_pods", "delete_pods"]



def kill_main_process(label_selector: str = None, name_pattern:
                      str = None, all: bool = False, rand:
                      bool = False, mode: str = "fixed", qty: int = 1,
                      ns: str = "default", order: str = "alphabetic",
                      container_name: str = "*", signal: str = "SIGTERM",
                      pumba_image: str = "gaiaadm/pumba:master",
                      wait_time: int = 35, secrets: Secrets = None):
    """
    Kill the main process in a pod's container. Select the appropriate pods
    by label and/or name patterns. Whenever a pattern is provided for the
    name, all pods retrieved will be filtered out if their name do not match
    the given pattern.
    If neither `label_selector` nor `name_pattern` are provided, all pods
    in the namespace will be selected to kill their container's main process.
    If `all` is set to `True`, all matching pods will terminate ther main
    containers processes.
    If `containe_name` is set to `*` all main processes in all containers of
    the pod will be killed. Otherwise, only the containers that match the
    given name in a pod.
    The parameter `signal` defines which kill signal is sent to the pod's main
    container. By default a `SIGTERM` signal is sent.
    The parameter `ns` defines the namespace in which pods are being selected.
    By default the `default` namespace is used.
    Value of `qty` varies based on `mode`.
    If `mode` is set to `fixed`, then `qty` refers to number of pods that kill
    their container's main processes. If `mode` is set to `percentage`, then
    `qty` refers to percentage of pods, from 1 to 100, that kill their
    container's main processes. Default `mode` is `fixed` and default `qty`
    is `1`.
    If `order` is set to `oldest`, the retrieved pods will be ordered
    by the pods creation_timestamp, with the oldest pod first in list.
    If `rand` is set to `True`, n random pods will be chosen to kill their
    container's main processes. Otherwise, the first retrieved n pods
    be chosen to kill their container's main process.
    With the parameter `pumba_image` one can change the image of pumba to
    delete processes. It defaults to the current master of the project.
    The parameter `wait_time` defines the amount of seconds (default `35`),
    that is waited until the pumba pods are checked for correct execution.
    """

    api = create_k8s_api_client(secrets)

    v1 = client.CoreV1Api(api)
    # determine the pods to kill
    pods_to_kill = _select_pods(v1=v1,label_selector=label_selector,
                                name_pattern=name_pattern,
                                all=all, rand=rand,
                                mode=mode, qty=qty,
                                ns=ns, order=order)

    # initiate pumba pods on the nodes which accommodate the pods whose
    # processes to kill


    i = 0
    timestamp = int(time.time())
    for pod in pods_to_kill:
        pumba_pod = client.V1Pod()
        pumba_pod.metadata = client.V1ObjectMeta(name="pumba-pod-%d-%d" %
                                                 (timestamp, i))
        pumba_pod.metadata.labels = {
            "app": "pumba",
            "com.gaiaadm.pumba": "true",
            "container": container_name,
            "pod": pod.metadata.name,
            "namespace": ns
        }
        container = client.V1Container(name="pumba")
        container.image = pumba_image
        container.image_pull_policy = "Always"
        container.args = ["--log-level", "debug", "kill", "--signal", signal,
                          "re2:^k8s_%s_%s_%s" % (container_name,
                                                 pod.metadata.name, ns)]
        resources = client.V1ResourceRequirements(
            requests={
                    "cpu": "10m",
                    "memory": "5M"
                },
            limits={
                    "cpu": "100m",
                    "memory": "20M"
                })
        container.resources = resources
        volume_mount = client.V1VolumeMount(mount_path="/var/run/docker.sock",
                                            name="dockersocket")
        container.volume_mounts = [volume_mount]
        spec = client.V1PodSpec(containers=[container])
        spec.restart_policy = "Never"
        spec.node_name = pod.spec.node_name
        host_path = client.V1HostPathVolumeSource(path="/var/run/docker.sock")
        volume = client.V1Volume(name="dockersocket", host_path=host_path)
        spec.volumes = [volume]
        pumba_pod.spec = spec
        v1.create_namespaced_pod(ns, pumba_pod)
        i += 1

    time.sleep(wait_time)

    # check every created pumba pod for correct execution and in case of no
    # error clean it up
    for j in range(0, i):
        pumba_pod = v1.read_namespaced_pod("pumba-pod-%d-%d" % (timestamp, j),
                                           ns)
        if pumba_pod.status.phase == "Succeeded":
            v1.delete_namespaced_pod("pumba-pod-%d-%d" % (timestamp, j), ns)
        else:
            logger.error("wasn't able to kill main process in container %s in\
                          pod %s" % (pumba_pod.metadata.labels['container'],
                                     pumba_pod.metadata.labels['pod']))
            return False


def terminate_pods(label_selector: str = None, name_pattern: str = None,
                   all: bool = False, rand: bool = False,
                   mode: str = "fixed", qty: int = 1,
                   grace_period: int = -1,
                   ns: str = "default", order: str = "alphabetic",
                   secrets: Secrets = None):
    """
    Terminate a pod gracefully. Select the appropriate pods by label and/or
    name patterns. Whenever a pattern is provided for the name, all pods
    retrieved will be filtered out if their name do not match the given
    pattern.

    If neither `label_selector` nor `name_pattern` are provided, all pods
    in the namespace will be selected for termination.

    If `all` is set to `True`, all matching pods will be terminated.

    Value of `qty` varies based on `mode`.
    If `mode` is set to `fixed`, then `qty` refers to number of pods to be
    terminated. If `mode` is set to `percentage`, then `qty` refers to
    percentage of pods, from 1 to 100, to be terminated.
    Default `mode` is `fixed` and default `qty` is `1`.

    If `order` is set to `oldest`, the retrieved pods will be ordered
    by the pods creation_timestamp, with the oldest pod first in list.

    If `rand` is set to `True`, n random pods will be terminated
    Otherwise, the first retrieved n pods will be terminated.

    If `grace_period` is greater than or equal to 0, it will
    be used as the grace period (in seconds) to terminate the pods.
    Otherwise, the default pod's grace period will be used.
    """

    api = create_k8s_api_client(secrets)
    v1 = client.CoreV1Api(api)

    pods = _select_pods(v1, label_selector, name_pattern,
                        all, rand, mode, qty, ns, order)

    body = client.V1DeleteOptions()
    if grace_period >= 0:
        body = client.V1DeleteOptions(grace_period_seconds=grace_period)

    for p in pods:
        v1.delete_namespaced_pod(p.metadata.name, ns, body=body)


def exec_in_pods(cmd: str,
                 label_selector: str = None, name_pattern: str = None,
                 all: bool = False, rand: bool = False,
                 mode: str = "fixed", qty: int = 1,
                 ns: str = "default", order: str = "alphabetic",
                 container_name: str = None,
                 request_timeout: int = 60,
                 secrets: Secrets = None) -> List[Dict[str, Any]]:
    """
    Execute the command `cmd` in the specified pod's container.
    Select the appropriate pods by label and/or name patterns.
    Whenever a pattern is provided for the name, all pods retrieved will be
    filtered out if their name do not match the given pattern.

    If neither `label_selector` nor `name_pattern` are provided, all pods
    in the namespace will be selected for termination.

    If `all` is set to `True`, all matching pods will be affected.

    Value of `qty` varies based on `mode`.
    If `mode` is set to `fixed`, then `qty` refers to number of pods affected.
    If `mode` is set to `percentage`, then `qty` refers to
    percentage of pods, from 1 to 100, to be affected.
    Default `mode` is `fixed` and default `qty` is `1`.

    If `order` is set to `oldest`, the retrieved pods will be ordered
    by the pods creation_timestamp, with the oldest pod first in list.

    If `rand` is set to `True`, n random pods will be affected
    Otherwise, the first retrieved n pods will be used
    """
    if not cmd:
        raise ActivityFailed("A command must be set to run a container")

    api = create_k8s_api_client(secrets)
    v1 = client.CoreV1Api(api)

    pods = _select_pods(v1, label_selector, name_pattern,
                        all, rand, mode, qty, ns, order)

    exec_command = cmd.strip().split()

    results = []
    for po in pods:
        logger.debug("Picked pods '{p}' for command execution {c}".format(
            p=po.metadata.name, c=exec_command))
        if not any(c.name == container_name for c in po.spec.containers):
            logger.debug("Pod {p} do not have container named '{n}'".format(
                p=po.metadata.name, n=container_name))
            continue

        pod_execution_result = {}
        # Use _preload_content to get back the raw JSON response.
        resp = stream.stream(v1.connect_get_namespaced_pod_exec,
                             po.metadata.name,
                             ns,
                             container=container_name,
                             command=exec_command,
                             stderr=True,
                             stdin=False,
                             stdout=True,
                             tty=False,
                             _preload_content=False)

        resp.run_forever(timeout=request_timeout)

        err = json.loads(resp.read_channel(ERROR_CHANNEL))
        out = resp.read_channel(STDOUT_CHANNEL)

        if err['status'] != "Success":
            error_code = err['details']['causes'][0]['message']
            error_message = err['message']
        else:
            error_code = 0
            error_message = ''

        results.append(dict(pod_name=po.metadata.name,
                            exit_code=error_code,
                            cmd=cmd,
                            stdout=out,
                            stderr=error_message))
    return results


###############################################################################
# Internals
###############################################################################
def _sort_by_pod_creation_timestamp(pod: V1Pod) -> datetime.datetime:
    """
    Function that serves as a key for the sort pods comparison
    """
    return pod.metadata.creation_timestamp


def _select_pods(v1: client.CoreV1Api = None, label_selector: str = None,
                 name_pattern: str = None,
                 all: bool = False, rand: bool = False,
                 mode: str = "fixed", qty: int = 1,
                 ns: str = "default",
                 order: str = "alphabetic") -> List[V1Pod]:

    # Fail if CoreV1Api is not instanciated
    if v1 is None:
        raise ActivityFailed("Cannot select pods. Client API is None")

    # Fail when quantity is less than 0
    if qty < 0:
        raise ActivityFailed(
            "Cannot select pods. Quantity '{q}' is negative.".format(q=qty))

    # Fail when mode is not `fixed` or `percentage`
    if mode not in ['fixed', 'percentage']:
        raise ActivityFailed(
            "Cannot select pods. Mode '{m}' is invalid.".format(m=mode))

    # Fail when order not `alphabetic` or `oldest`
    if order not in ['alphabetic', 'oldest']:
        raise ActivityFailed(
            "Cannot select pods. Order '{o}' is invalid.".format(o=order))

    if label_selector:
        ret = v1.list_namespaced_pod(ns, label_selector=label_selector)
        logger.debug("Found {d} pods labelled '{s}' in ns {n}".format(
            d=len(ret.items), s=label_selector, n=ns))
    else:
        ret = v1.list_namespaced_pod(ns)
        logger.debug("Found {d} pods in ns '{n}'".format(
            d=len(ret.items), n=ns))

    pods = []
    if name_pattern:
        pattern = re.compile(name_pattern)
        for p in ret.items:
            if pattern.match(p.metadata.name):
                pods.append(p)
                logger.debug("Pod '{p}' match pattern".format(
                    p=p.metadata.name))
    else:
        pods = ret.items

    if order == 'oldest':
        pods.sort(key=_sort_by_pod_creation_timestamp)
    if not all:
        if mode == 'percentage':
            qty = math.ceil((qty * len(pods)) / 100)
        # If quantity is greater than number of pods present, cap the
        # quantity to maximum number of pods
        qty = min(qty, len(pods))

        if rand:
            pods = random.sample(pods, qty)
        else:
            pods = pods[:qty]

    return pods


def delete_pods(name: str, ns: str = "default",
                label_selector: str = "name in ({name})",
                secrets: Secrets = None):
    """
    Delete pods by `name` in the namespace `ns`.

    The pods are deleted without a graceful period to trigger an abrupt
    termination.

    The selected resources are matched by the given `label_selector`.
    """
    label_selector = label_selector.format(name=name)
    api = create_k8s_api_client(secrets)
    v1 = client.CoreV1Api(api)
    if label_selector:
        ret = v1.list_namespaced_pod(ns, label_selector=label_selector)
    else:
        ret = v1.list_namespaced_pod(ns)

    logger.debug("Found {d} pods named '{n}'".format(
        d=len(ret.items), n=name))

    body = client.V1DeleteOptions()
    for p in ret.items:
        v1.delete_namespaced_pod(p.metadata.name, ns, body=body)
