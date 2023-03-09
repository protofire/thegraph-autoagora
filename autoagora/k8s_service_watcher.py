# Copyright 2022-, Semiotic AI, Inc.
# SPDX-License-Identifier: Apache-2.0

import asyncio as aio
import logging

from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client.api_client import ApiClient


class K8SServiceEndpointsWatcher:
    def __init__(self, service_name: str) -> None:
        """Maintains an automatically, asynchronously updated list of endpoints backing
        a kubernetes service in the current namespace.

        This is supposed to be run from within a Kubernetes pod. The pod will need a
        role that grants it:

        ```
            rules:
            - apiGroups: [""]
                resources: ["endpoints"]
                verbs: ["watch"]
        ```

        Args:
            service_name (str): Kubernetes service name.

        Raises:
            FileNotFoundError: couldn't find
            `/var/run/secrets/kubernetes.io/serviceaccount/namespace`, which is
            expected when running within a Kubernetes pod container.
        """
        self.endpoint_ips = []
        self._service_name = service_name

        try:
            with open(
                "/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r"
            ) as f:
                self._namespace = f.read().strip()
        except FileNotFoundError:
            logging.exception("Probably not running in Kubernetes.")
            raise

        # Starts the async _loop immediately
        self._future = aio.ensure_future(self._loop())

    async def _loop(self) -> None:
        config.load_incluster_config()

        async with ApiClient() as api:
            v1 = client.CoreV1Api(api)
            w = watch.Watch()
            async for event in w.stream(
                v1.list_namespaced_endpoints,
                namespace=self._namespace,
                field_selector=f"metadata.name={self._service_name}",
            ):
                result = event["object"]  # type: ignore

                self.endpoint_ips = [
                    address.ip
                    for subset in result.subsets  # type: ignore
                    for address in subset.addresses  # type: ignore
                ]

                logging.debug(
                    "Found new endpoint IPs for service %s: %s",
                    self._service_name,
                    self.endpoint_ips,
                )
