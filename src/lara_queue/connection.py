"""
Redis connection utilities with optimized pooling for production use.
"""
import logging
from typing import Optional

from redis import Redis
from redis.connection import ConnectionPool

logger = logging.getLogger(__name__)


class RedisConnectionFactory:
    """
    Factory for creating Redis connections with production-ready settings.
    """

    @staticmethod
    def create_connection_pool(
        redis_url: str = "redis://localhost:6379/0",
        max_connections: int = 50,
        socket_timeout: int = 60,
        socket_connect_timeout: int = 10,
        socket_keepalive: bool = True,
        socket_keepalive_options: Optional[dict] = None,
        retry_on_timeout: bool = True,
        health_check_interval: int = 30,
        **kwargs
    ) -> ConnectionPool:
        """
        Create a Redis connection pool with recommended settings for production.

        Args:
            redis_url: Redis connection URL (e.g., "redis://localhost:6379/0")
            max_connections: Maximum connections in the pool
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Socket connect timeout in seconds
            socket_keepalive: Enable TCP keepalive
            socket_keepalive_options: TCP keepalive options
            retry_on_timeout: Retry operations on timeout
            health_check_interval: Health check interval in seconds
            **kwargs: Additional connection pool arguments

        Returns:
            Configured ConnectionPool instance

        Example:
            >>> pool = RedisConnectionFactory.create_connection_pool(
            ...     redis_url="redis://localhost:6379/0",
            ...     max_connections=100
            ... )
            >>> client = Redis(connection_pool=pool)
        """
        if socket_keepalive_options is None:
            # Default TCP keepalive options for Linux
            socket_keepalive_options = {
                # Start sending keepalive probes after 60 seconds of idle
                1: 60,  # TCP_KEEPIDLE
                # Send keepalive probes every 10 seconds
                2: 10,  # TCP_KEEPINTVL
                # Close connection after 6 failed probes
                3: 6,   # TCP_KEEPCNT
            }

        pool = ConnectionPool.from_url(
            redis_url,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options if socket_keepalive else None,
            retry_on_timeout=retry_on_timeout,
            health_check_interval=health_check_interval,
            **kwargs
        )

        logger.info(
            f"Created Redis connection pool: url={redis_url}, "
            f"max_connections={max_connections}, "
            f"socket_timeout={socket_timeout}s"
        )

        return pool

    @staticmethod
    def create_client(
        redis_url: str = "redis://localhost:6379/0",
        max_connections: int = 50,
        socket_timeout: int = 60,
        **kwargs
    ) -> Redis:
        """
        Create a Redis client with optimized connection pool.

        Args:
            redis_url: Redis connection URL
            max_connections: Maximum connections in the pool
            socket_timeout: Socket timeout in seconds
            **kwargs: Additional arguments passed to create_connection_pool

        Returns:
            Configured Redis client instance

        Example:
            >>> client = RedisConnectionFactory.create_client(
            ...     redis_url="redis://localhost:6379/0",
            ...     max_connections=100
            ... )
        """
        pool = RedisConnectionFactory.create_connection_pool(
            redis_url=redis_url,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            **kwargs
        )

        return Redis(connection_pool=pool)


# Convenience function for quick setup
def create_redis_client(
    redis_url: str = "redis://localhost:6379/0",
    max_connections: int = 50,
    **kwargs
) -> Redis:
    """
    Convenience function to create a Redis client with production settings.

    Args:
        redis_url: Redis connection URL
        max_connections: Maximum connections in the pool
        **kwargs: Additional connection pool arguments

    Returns:
        Configured Redis client

    Example:
        >>> from lara_queue.connection import create_redis_client
        >>> client = create_redis_client("redis://localhost:6379/0")
    """
    return RedisConnectionFactory.create_client(
        redis_url=redis_url,
        max_connections=max_connections,
        **kwargs
    )
