# Copyright 2022-, Semiotic AI, Inc.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import re
from functools import reduce

import aiohttp
import backoff

from autoagora.config import args


class HTTPError(Exception):
    """Catch-all for HTTP errors"""


@backoff.on_exception(
    backoff.expo, (aiohttp.ClientError, HTTPError), max_time=30, logger=logging.root
)
async def subgraph_query_count(subgraph: str) -> int:
    endpoints = args.indexer_service_metrics_endpoint.split(",")
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

    if len(results) == 0:
        # The subgraph query count will not be in the metric if it hasn't received any
        # queries.
        return 0
    if len(results) == 1:
        return int(results[0])
    else:
        return reduce(lambda x, y: int(x) + int(y), results)


if __name__ == "__main__":
    res = asyncio.run(
        subgraph_query_count("Qmaz1R8vcv9v3gUfksqiS9JUz7K9G8S5By3JYn8kTiiP5K")
    )
    print(res)
