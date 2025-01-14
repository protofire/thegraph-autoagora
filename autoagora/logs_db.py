# Copyright 2022-, Semiotic AI, Inc.
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass

import asyncpg


class LogsDB:
    @dataclass
    class QueryStats:
        query: str
        count: int
        min_time: int
        max_time: int
        avg_time: float
        stddev_time: float

    def __init__(self, pgpool: asyncpg.Pool) -> None:
        self.pgpool = pgpool

    async def get_most_frequent_queries(
        self, subgraph_ipfs_hash: str, min_count: int = 100
    ):
        async with self.pgpool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT
                    query,
                    count_id,
                    min_time,
                    max_time,
                    avg_time,
                    stddev_time
                FROM
                    query_skeletons
                INNER JOIN
                (
                    SELECT
                        query_hash as qhash,
                        count(id) as count_id,
                        Min(query_time_ms) as min_time,
                        Max(query_time_ms) as max_time,
                        Avg(query_time_ms) as avg_time,
                        Stddev(query_time_ms) as stddev_time 
                    FROM
                        query_logs
                    WHERE
                        subgraph = $1
                        AND query_time_ms IS NOT NULL
                    GROUP BY
                        qhash
                    HAVING
                        Count(id) >= $2
                ) as query_logs
                ON
                    qhash = hash
                ORDER BY
                    count_id DESC
                """,
                subgraph_ipfs_hash,
                min_count,
            )

        return [
            LogsDB.QueryStats(
                query=row[0],
                count=row[1],
                min_time=row[2],
                max_time=row[3],
                avg_time=float(row[4]),
                stddev_time=float(row[5]),
            )
            for row in rows
        ]

    async def get_subgraph_average_query_stats(self, subgraph_ipfs_hash: str):
        async with self.pgpool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    count(id),
                    Min(query_time_ms),
                    Max(query_time_ms),
                    Avg(query_time_ms),
                    Stddev(query_time_ms) 
                FROM
                    query_logs
                WHERE
                    subgraph = $1
                    AND query_time_ms IS NOT NULL
                """,
                subgraph_ipfs_hash,
            )

        assert row

        logging.debug(
            "Subgraph average query stats for subgraph %s: %s", subgraph_ipfs_hash, row
        )

        return LogsDB.QueryStats(
            query="default",
            count=row[0],
            min_time=row[1],
            max_time=row[2],
            avg_time=float(row[3]),
            stddev_time=float(row[4]),
        )

    async def get_frequent_query_hashes_without_timing(self, min_count: int = 100):
        async with self.pgpool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT
                    query_hash
                FROM
                    query_logs
                GROUP BY
                    query_hash
                HAVING
                    Count(id) >= $1
                    AND Sum(CASE WHEN query_time_ms IS NOT NULL THEN 1 ELSE 0 END) < $2
                """,
                min_count,
                min_count,
            )

        logging.debug("Frequent query hashes: %s", rows)

        return list(bytes(row[0]).hex() for row in rows)
