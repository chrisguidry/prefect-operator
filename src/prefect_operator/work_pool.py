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
                    f'--name "{ self.namespace }:${{HOSTNAME}}"'
                ),
            ],
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
                pass
            case 404:
                response = client.post(
                    "/work_pools/",
                    json={
                        "name": work_pool.work_pool_name,
                        "type": "kubernetes",
                    },
                )
                response.raise_for_status()
            case _:
                response.raise_for_status()

    logger.info("Created work pool %s", work_pool.work_pool_name)

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

    with work_pool.server.in_cluster_client() as client:
        response = client.delete(f"/work_pools/{work_pool.work_pool_name}")
        if response.status_code not in (200, 204, 404):
            response.raise_for_status()
        logger.info("Deleted work pool %s", work_pool.work_pool_name)
