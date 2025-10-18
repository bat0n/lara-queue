"""
Async Redis connection utilities with optimized pooling for production use.
"""
import logging
from typing import Optional

import aioredis

logger = logging.getLogger(__name__)


class AsyncRedisConnectionFactory:
    """
    Factory for creating async Redis connections with production-ready settings.
    """

    @staticmethod
    async def create_client(
        redis_url: str = "redis://localhost:6379/0",
        max_connections: int = 50,
        socket_timeout: int = 60,
        socket_connect_timeout: int = 10,
        socket_keepalive: bool = True,
        retry_on_timeout: bool = True,
        health_check_interval: int = 30,
        decode_responses: bool = False,
        **kwargs
    ) -> aioredis.Redis:
        """
        Create an async Redis client with optimized connection pool.

        Args:
            redis_url: Redis connection URL (e.g., "redis://localhost:6379/0")
            max_connections: Maximum connections in the pool
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Socket connect timeout in seconds
            socket_keepalive: Enable TCP keepalive
            retry_on_timeout: Retry operations on timeout
            health_check_interval: Health check interval in seconds
            decode_responses: Decode byte responses to strings
            **kwargs: Additional connection arguments

        Returns:
            Configured async Redis client instance

        Example:
            >>> import asyncio
            >>> from lara_queue.async_connection import AsyncRedisConnectionFactory
            >>>
            >>> async def main():
            ...     client = await AsyncRedisConnectionFactory.create_client(
            ...         redis_url="redis://localhost:6379/0",
            ...         max_connections=100
            ...     )
            ...     return client
            >>>
            >>> client = asyncio.run(main())
        """
        client = await aioredis.from_url(
            redis_url,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            retry_on_timeout=retry_on_timeout,
            health_check_interval=health_check_interval,
            decode_responses=decode_responses,
            **kwargs
        )

        logger.info(
            f"Created async Redis client: url={redis_url}, "
            f"max_connections={max_connections}, "
            f"socket_timeout={socket_timeout}s"
        )

        return client


# Convenience function for quick setup
async def create_async_redis_client(
    redis_url: str = "redis://localhost:6379/0",
    max_connections: int = 50,
    **kwargs
) -> aioredis.Redis:
    """
    Convenience function to create an async Redis client with production settings.

    Args:
        redis_url: Redis connection URL
        max_connections: Maximum connections in the pool
        **kwargs: Additional connection arguments

    Returns:
        Configured async Redis client

    Example:
        >>> import asyncio
        >>> from lara_queue.async_connection import create_async_redis_client
        >>>
        >>> async def main():
        ...     client = await create_async_redis_client("redis://localhost:6379/0")
        ...     return client
        >>>
        >>> client = asyncio.run(main())
    """
    return await AsyncRedisConnectionFactory.create_client(
        redis_url=redis_url,
        max_connections=max_connections,
        **kwargs
    )
