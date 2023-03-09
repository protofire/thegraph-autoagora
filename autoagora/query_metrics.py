# Copyright 2022-, Semiotic AI, Inc.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import re
from abc import ABC, abstractmethod
from functools import reduce
from typing import List
from urllib.parse import urlparse

import aiohttp
import backoff

from autoagora.k8s_service_watcher import K8SServiceEndpointsWatcher


class MetricsEndpoints(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def __call__(self) -> List[str]:
        pass


class StaticMetricsEndpoints(MetricsEndpoints):
    def __init__(self, comma_separated_endpoints: str) -> None:
        super().__init__()
        self._endpoints = comma_separated_endpoints.split(",")

    def __call__(self) -> List[str]:
        return self._endpoints


class K8SServiceWatcherMetricsEndpoints(MetricsEndpoints):
    def __init__(self, url: str) -> None:
        super().__init__()
        self._parsed_url = urlparse(url)
        # Assuming the "hostname" is actually the k8s service name, as indicated in the
        # arguments documentation.

        service_name = self._parsed_url.hostname
        # Check that service_name is non-empty
        assert service_name
        # Check that service_name is a valid RFC-1123 DNS label
        assert re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", service_name)
        self._k8s_service_watcher = K8SServiceEndpointsWatcher(service_name)

    def __call__(self) -> List[str]:
        port = self._parsed_url.port
        return [
            self._parsed_url._replace(netloc=f"{endpoint_ip}:{port}").geturl()
            for endpoint_ip in self._k8s_service_watcher.endpoint_ips
        ]


class HTTPError(Exception):
    """Catch-all for HTTP errors"""


@backoff.on_exception(
    backoff.expo, (aiohttp.ClientError, HTTPError), max_time=30, logger=logging.root
)
async def subgraph_query_count(
    subgraph: str, metrics_endpoints: MetricsEndpoints
) -> int:
    endpoints = metrics_endpoints()
    results = []
    async with aiohttp.ClientSession() as session:
        for endpoint in endpoints:
            async with session.get(endpoint) as response:
                if response.status != 200:
                    raise HTTPError(response.status)

                results.extend(
                    re.findall(
                        r'indexer_service_queries_ok{{deployment="{subgraph}"}} ([0-9]*)'.format(
                            subgraph=subgraph
                        ),
                        await response.text(),
                    )
                )

                logging.debug(
                    "Number of queries for subgraph %s from %s: %s",
                    subgraph,
                    endpoint,
                    results[-1:],  # Will return empty list if empty, instead of error
                )

    if len(results) == 0:
        # The subgraph query count will not be in the metric if it hasn't received any
        # queries.
        return 0
    if len(results) == 1:
        return int(results[0])
    else:
        return reduce(lambda x, y: int(x) + int(y), results)
