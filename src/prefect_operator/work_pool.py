import time
from contextlib import contextmanager
from typing import Any, Generator

import httpx
import kopf
import kubernetes
from pydantic import BaseModel, Field

from .common import KubernetesPortForwardTransport, NamedResource


class PrefectServerReference(BaseModel):
    namespace: str = Field("")
    name: str

    @property
    def as_environment_variable(self) -> dict[str, Any]:
        return {"name": "PREFECT_API_URL", "value": self.in_cluster_api_url}

    @property
    def in_cluster_api_url(self) -> str:
        return f"http://{self.name}.{self.namespace}.svc:4200/api"

    @contextmanager
    def in_cluster_client(self) -> Generator[httpx.Client, None, None]:
        # TODO: figure out how to detect whether we're inside or outside the cluster
        # and whether we need the port-forwarding transport
        with httpx.Client(
            base_url=self.in_cluster_api_url,
            transport=KubernetesPortForwardTransport(),
        ) as c:
            while True:
                try:
                    response = c.get("/health")
                    response.raise_for_status()
                    break
                except Exception as e:
                    print(e)
                    time.sleep(1)

            yield c


class PrefectWorkPool(NamedResource):
    server: PrefectServerReference
    workers: int = Field(1)
    serviceAccount: str = Field("default")

    @property
    def work_pool_name(self) -> str:
        return f"{self.namespace}:{self.name}"

    def desired_deployment(self) -> dict[str, Any]:
        container_template = {
            "name": "prefect-worker",
            "image": "prefecthq/prefect:3.0.0rc2-python3.12-kubernetes",
            "env": [
                self.server.as_environment_variable,
            ],
            "command": [
                "bash",
                "-c",
                (
                    "prefect worker start --type kubernetes "
                    f"--pool '{ self.work_pool_name }' "
                    f'--name "{ self.namespace }:${{HOSTNAME}}" '
                    "--with-healthcheck"
                ),
            ],
            "readinessProbe": {
                "httpGet": {"path": "/health", "port": 8080, "scheme": "HTTP"},
                "initialDelaySeconds": 5,
                "periodSeconds": 5,
                "timeoutSeconds": 5,
                "successThreshold": 1,
                "failureThreshold": 30,
            },
            "livenessProbe": {
                "httpGet": {"path": "/health", "port": 8080, "scheme": "HTTP"},
                "initialDelaySeconds": 30,
                "periodSeconds": 15,
                "timeoutSeconds": 5,
                "successThreshold": 1,
                "failureThreshold": 2,
            },
        }

        pod_template: dict[str, Any] = {
            "metadata": {"labels": {"app": self.name}},
            "spec": {
                "containers": [container_template],
            },
        }

        deployment_spec = {
            "replicas": self.workers,
            "selector": {"matchLabels": {"app": self.name}},
            "template": pod_template,
        }

        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"namespace": self.namespace, "name": self.name},
            "spec": deployment_spec,
        }

    @property
    def cluster_role_name(self) -> str:
        return "prefect-worker"

    @property
    def role_name(self) -> str:
        return f"prefect-worker-{self.name}"

    def desired_role(self) -> dict[str, Any]:
        return {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {
                "namespace": self.namespace,
                "name": self.role_name,
            },
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["pods", "pods/log", "pods/status"],
                    "verbs": ["get", "watch", "list"],
                },
                {
                    "apiGroups": ["batch"],
                    "resources": ["jobs"],
                    "verbs": [
                        "get",
                        "list",
                        "watch",
                        "create",
                        "update",
                        "patch",
                        "delete",
                    ],
                },
            ],
        }

    def desired_role_bindings(self) -> list[dict[str, Any]]:
        return [
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": {
                    "name": f"{self.namespace}--{self.role_name}--{self.serviceAccount}",
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "namespace": self.namespace,
                        "name": self.serviceAccount,
                    }
                ],
                "roleRef": {
                    "kind": "ClusterRole",
                    "name": self.cluster_role_name,
                    "apiGroup": "rbac.authorization.k8s.io",
                },
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "namespace": self.namespace,
                    "name": f"{self.role_name}--{self.serviceAccount}",
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "namespace": self.namespace,
                        "name": self.serviceAccount,
                    }
                ],
                "roleRef": {
                    "kind": "Role",
                    "name": self.role_name,
                    "apiGroup": "rbac.authorization.k8s.io",
                },
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "namespace": self.namespace,
                    "name": f"{self.role_name}--{self.serviceAccount}--kube-system",
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "namespace": self.namespace,
                        "name": self.serviceAccount,
                    }
                ],
                "roleRef": {
                    "kind": "Role",
                    "name": self.role_name,
                    "apiGroup": "rbac.authorization.k8s.io",
                },
            },
        ]

    def desired_base_job_template(self) -> dict[str, Any]:
        return {
            "variables": {
                "type": "object",
                "properties": {
                    "env": {
                        "type": "object",
                        "title": "Environment Variables",
                        "description": "Environment variables to set when starting a flow run.",
                        "additionalProperties": {"type": "string"},
                    },
                    "name": {
                        "type": "string",
                        "title": "Name",
                        "description": "Name given to infrastructure created by a worker.",
                    },
                    "image": {
                        "type": "string",
                        "title": "Image",
                        "example": "prefecthq/prefect:3.0.0rc2-python3.12-kubernetes",
                        "description": "The image reference of a container image to use for created jobs. If not set, the latest Prefect image will be used.",
                        "default": "prefecthq/prefect:3.0.0rc2-python3.12-kubernetes",
                    },
                    "labels": {
                        "type": "object",
                        "title": "Labels",
                        "description": "Labels applied to infrastructure created by a worker.",
                        "additionalProperties": {"type": "string"},
                    },
                    "command": {
                        "type": "string",
                        "title": "Command",
                        "description": "The command to use when starting a flow run. In most cases, this should be left blank and the command will be automatically generated by the worker.",
                    },
                    "namespace": {
                        "type": "string",
                        "title": "Namespace",
                        "default": self.namespace,
                        "description": "The Kubernetes namespace to create jobs within.",
                    },
                    "stream_output": {
                        "type": "boolean",
                        "title": "Stream Output",
                        "default": True,
                        "description": "If set, output will be streamed from the job to local standard output.",
                    },
                    "cluster_config": {
                        "allOf": [{"$ref": "#/definitions/KubernetesClusterConfig"}],
                        "title": "Cluster Config",
                        "description": "The Kubernetes cluster config to use for job creation.",
                    },
                    "finished_job_ttl": {
                        "type": "integer",
                        "title": "Finished Job TTL",
                        "description": "The number of seconds to retain jobs after completion. If set, finished jobs will be cleaned up by Kubernetes after the given delay. If not set, jobs will be retained indefinitely.",
                    },
                    "image_pull_policy": {
                        "enum": ["IfNotPresent", "Always", "Never"],
                        "type": "string",
                        "title": "Image Pull Policy",
                        "default": "IfNotPresent",
                        "description": "The Kubernetes image pull policy to use for job containers.",
                    },
                    "service_account_name": {
                        "type": "string",
                        "title": "Service Account Name",
                        "description": "The Kubernetes service account to use for job creation.",
                    },
                    "job_watch_timeout_seconds": {
                        "type": "integer",
                        "title": "Job Watch Timeout Seconds",
                        "description": "Number of seconds to wait for each event emitted by a job before timing out. If not set, the worker will wait for each event indefinitely.",
                    },
                    "pod_watch_timeout_seconds": {
                        "type": "integer",
                        "title": "Pod Watch Timeout Seconds",
                        "default": 60,
                        "description": "Number of seconds to watch for pod creation before timing out.",
                    },
                },
                "definitions": {
                    "KubernetesClusterConfig": {
                        "type": "object",
                        "title": "KubernetesClusterConfig",
                        "required": ["config", "context_name"],
                        "properties": {
                            "config": {
                                "type": "object",
                                "title": "Config",
                                "description": "The entire contents of a kubectl config file.",
                            },
                            "context_name": {
                                "type": "string",
                                "title": "Context Name",
                                "description": "The name of the kubectl context to use.",
                            },
                        },
                        "description": "Stores configuration for interaction with Kubernetes clusters.\n\nSee `from_file` for creation.",
                        "secret_fields": [],
                        "block_type_slug": "kubernetes-cluster-config",
                        "block_schema_references": {},
                    }
                },
                "description": "Default variables for the Kubernetes worker.\n\nThe schema for this class is used to populate the `variables` section of the default\nbase job template.",
            },
            "job_configuration": {
                "env": "{{ env }}",
                "name": "{{ name }}",
                "labels": "{{ labels }}",
                "command": "{{ command }}",
                "namespace": "{{ namespace }}",
                "job_manifest": {
                    "kind": "Job",
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "env": "{{ env }}",
                                        "args": "{{ command }}",
                                        "name": "prefect-job",
                                        "image": "{{ image }}",
                                        "imagePullPolicy": "{{ image_pull_policy }}",
                                    }
                                ],
                                "completions": 1,
                                "parallelism": 1,
                                "restartPolicy": "Never",
                                "serviceAccountName": "{{ service_account_name }}",
                            }
                        },
                        "backoffLimit": 0,
                        "ttlSecondsAfterFinished": "{{ finished_job_ttl }}",
                    },
                    "metadata": {
                        "labels": "{{ labels }}",
                        "namespace": "{{ namespace }}",
                        "generateName": "{{ name }}-",
                    },
                    "apiVersion": "batch/v1",
                },
                "stream_output": "{{ stream_output }}",
                "cluster_config": "{{ cluster_config }}",
                "job_watch_timeout_seconds": "{{ job_watch_timeout_seconds }}",
                "pod_watch_timeout_seconds": "{{ pod_watch_timeout_seconds }}",
            },
        }


@kopf.on.resume("prefect.io", "v3", "prefectworkpool")
@kopf.on.create("prefect.io", "v3", "prefectworkpool")
@kopf.on.update("prefect.io", "v3", "prefectworkpool")
def reconcile_work_pool(
    namespace: str, name: str, spec: dict[str, Any], logger: kopf.Logger, **_
):
    work_pool = PrefectWorkPool.model_validate(
        spec, context={"name": name, "namespace": namespace}
    )
    print(repr(work_pool))

    with work_pool.server.in_cluster_client() as client:
        response = client.get(f"/work_pools/{work_pool.work_pool_name}")
        match response.status_code:
            case 200:
                response = client.patch(
                    f"/work_pools/{work_pool.work_pool_name}",
                    json={"base_job_template": work_pool.desired_base_job_template()},
                )
                response.raise_for_status()
                logger.info("Updated work pool %s", work_pool.work_pool_name)
            case 404:
                response = client.post(
                    "/work_pools/",
                    json={
                        "name": work_pool.work_pool_name,
                        "type": "kubernetes",
                        "base_job_template": work_pool.desired_base_job_template(),
                    },
                )
                response.raise_for_status()
                logger.info("Created work pool %s", work_pool.work_pool_name)

            case _:
                response.raise_for_status()

    create_prefect_worker_roles(work_pool, logger)

    api = kubernetes.client.AppsV1Api()
    desired_deployment = work_pool.desired_deployment()

    try:
        api.create_namespaced_deployment(
            work_pool.namespace,
            desired_deployment,
        )
        logger.info("Created deployment %s", name)
    except kubernetes.client.ApiException as e:
        if e.status != 409:
            raise

        api.replace_namespaced_deployment(
            desired_deployment["metadata"]["name"],
            work_pool.namespace,
            desired_deployment,
        )
        logger.info("Updated deployment %s", name)


@kopf.on.delete("prefect.io", "v3", "prefectworkpool")
def delete_work_pool(
    namespace: str, name: str, spec: dict[str, Any], logger: kopf.Logger, **_
):
    work_pool = PrefectWorkPool.model_validate(
        spec, context={"name": name, "namespace": namespace}
    )
    print(repr(work_pool))

    api = kubernetes.client.AppsV1Api()
    try:
        api.delete_namespaced_deployment(name, namespace)
        logger.info("Deleted deployment %s", name)
    except kubernetes.client.ApiException as e:
        if e.status == 404:
            logger.info("deployment %s not found", name)
        else:
            raise

    delete_worker_roles(work_pool, logger)

    with work_pool.server.in_cluster_client() as client:
        response = client.delete(f"/work_pools/{work_pool.work_pool_name}")
        if response.status_code not in (200, 204, 404):
            response.raise_for_status()
        logger.info("Deleted work pool %s", work_pool.work_pool_name)


def create_prefect_worker_roles(work_pool: PrefectWorkPool, logger: kopf.Logger):
    api = kubernetes.client.RbacAuthorizationV1Api()

    desired_role = work_pool.desired_role()
    try:
        api.create_namespaced_role(work_pool.namespace, desired_role)
        logger.info("Created role %s", work_pool.role_name)
    except kubernetes.client.ApiException as e:
        if e.status != 409:
            raise

        api.replace_namespaced_role(
            work_pool.role_name,
            work_pool.namespace,
            desired_role,
        )
        logger.info("Updated role %s", work_pool.role_name)

    for desired_role_binding in work_pool.desired_role_bindings():
        match desired_role_binding["kind"]:
            case "ClusterRoleBinding":
                try:
                    api.create_cluster_role_binding(desired_role_binding)
                    logger.info(
                        "Created cluster role binding %s",
                        desired_role_binding["metadata"]["name"],
                    )
                except kubernetes.client.ApiException as e:
                    if e.status != 409:
                        raise

                    api.replace_cluster_role_binding(
                        desired_role_binding["metadata"]["name"],
                        desired_role_binding,
                    )
                    logger.info(
                        "Updated cluster role binding %s",
                        desired_role_binding["metadata"]["name"],
                    )
            case "RoleBinding":
                try:
                    api.create_namespaced_role_binding(
                        work_pool.namespace,
                        desired_role_binding,
                    )
                    logger.info(
                        "Created role binding %s",
                        desired_role_binding["metadata"]["name"],
                    )
                except kubernetes.client.ApiException as e:
                    if e.status != 409:
                        raise

                    api.replace_namespaced_role_binding(
                        desired_role_binding["metadata"]["name"],
                        work_pool.namespace,
                        desired_role_binding,
                    )
                    logger.info(
                        "Updated role binding %s",
                        desired_role_binding["metadata"]["name"],
                    )
            case _:
                raise ValueError(f"Unknown kind {desired_role_binding['kind']}")


def delete_worker_roles(work_pool: PrefectWorkPool, logger: kopf.Logger):
    api = kubernetes.client.RbacAuthorizationV1Api()

    for desired_role_binding in work_pool.desired_role_bindings():
        try:
            match desired_role_binding["kind"]:
                case "ClusterRoleBinding":
                    api.delete_cluster_role_binding(
                        desired_role_binding["metadata"]["name"],
                    )
                    logger.info(
                        "Deleted cluster role binding %s",
                        desired_role_binding["metadata"]["name"],
                    )
                case "RoleBinding":
                    api.delete_namespaced_role_binding(
                        desired_role_binding["metadata"]["name"],
                        work_pool.namespace,
                    )
                    logger.info(
                        "Deleted role binding %s",
                        desired_role_binding["metadata"]["name"],
                    )
                case _:
                    raise ValueError(f"Unknown kind {desired_role_binding['kind']}")
        except kubernetes.client.ApiException as e:
            if e.status != 404:
                raise

    try:
        api.delete_namespaced_role(work_pool.role_name, work_pool.namespace)
        logger.info("Deleted role %s", work_pool.role_name)
    except kubernetes.client.ApiException as e:
        if e.status != 404:
            raise
