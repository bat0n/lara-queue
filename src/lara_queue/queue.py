from redis import Redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    RedisError
)
import json
from .module import phpserialize
from pyee.base import EventEmitter
from .retry_strategy import RetryStrategy
from .metrics_collector import MetricsCollector
from .base_queue import BaseQueue
from .connection import RedisConnectionFactory
import uuid
import time
import logging
import signal
import sys
from typing import Dict, List, Optional, Union, Any, Callable, Tuple

# Setup logger
logger = logging.getLogger(__name__)


class Queue(BaseQueue):
    """
    Synchronous queue implementation for Laravel queue synchronization.

    Inherits common functionality from BaseQueue and implements
    synchronous Redis operations.
    """

    def __init__(self,
                 client: Redis,
                 queue: str,
                 driver: str = 'redis',
                 appname: str = 'laravel',
                 prefix: str = '_database_',
                 is_queue_notify: bool = True,
                 is_horizon: bool = False,
                 horizon_metrics_enabled: bool = True,
                 horizon_ttl: int = 86400,
                 dead_letter_queue: Optional[str] = None,
                 max_retries: int = 3,
                 retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
                 retry_delay: int = 5,
                 retry_max_delay: int = 300,
                 retry_jitter: bool = True,
                 retry_backoff_multiplier: float = 2.0,
                 retry_custom_function: Optional[Callable[[int], int]] = None,
                 retry_exceptions: Optional[List[type]] = None,
                 enable_metrics: bool = True,
                 metrics_history_size: int = 1000,
                 retry_tracking_ttl: int = 3600,
                 max_retry_tracking: int = 10000) -> None:
        """
        Initialize synchronous Queue.

        Args:
            client: Redis client instance
            queue: Queue name
            driver: Driver type (currently only 'redis')
            appname: Laravel application name
            prefix: Queue prefix
            is_queue_notify: Enable queue notifications
            is_horizon: Enable Laravel Horizon support
            horizon_metrics_enabled: Enable Horizon metrics collection
            horizon_ttl: TTL for Horizon metrics (seconds)
            dead_letter_queue: Dead letter queue name
            max_retries: Maximum retry attempts
            retry_strategy: Retry strategy to use
            retry_delay: Initial retry delay (seconds)
            retry_max_delay: Maximum retry delay (seconds)
            retry_jitter: Add jitter to retry delays
            retry_backoff_multiplier: Backoff multiplier for exponential strategy
            retry_custom_function: Custom retry delay function
            retry_exceptions: List of retryable exception types
            enable_metrics: Enable metrics collection
            metrics_history_size: Maximum history size for metrics
            retry_tracking_ttl: TTL for retry tracking entries (seconds)
            max_retry_tracking: Maximum retry tracking entries before cleanup
        """
        # Initialize base class
        super().__init__(
            queue=queue,
            driver=driver,
            appname=appname,
            prefix=prefix,
            is_queue_notify=is_queue_notify,
            is_horizon=is_horizon,
            horizon_metrics_enabled=horizon_metrics_enabled,
            horizon_ttl=horizon_ttl,
            dead_letter_queue=dead_letter_queue,
            max_retries=max_retries,
            retry_strategy=retry_strategy,
            retry_delay=retry_delay,
            retry_max_delay=retry_max_delay,
            retry_jitter=retry_jitter,
            retry_backoff_multiplier=retry_backoff_multiplier,
            retry_custom_function=retry_custom_function,
            retry_exceptions=retry_exceptions,
            enable_metrics=enable_metrics,
            metrics_history_size=metrics_history_size,
            retry_tracking_ttl=retry_tracking_ttl,
            max_retry_tracking=max_retry_tracking,
        )

        self.client = client
        self.ee = EventEmitter()

        # Synchronous queue-specific flags
        self._processing_job = False

        # Initialize metrics collector
        self.metrics = MetricsCollector(max_history_size=metrics_history_size) if enable_metrics else None

    @classmethod
    def create_with_recommended_settings(
        cls,
        redis_url: str = "redis://localhost:6379/0",
        queue: str = "default",
        max_connections: int = 50,
        socket_timeout: int = 60,
        **kwargs
    ) -> 'Queue':
        """
        Create Queue with production-ready Redis connection settings.

        Args:
            redis_url: Redis connection URL
            queue: Queue name
            max_connections: Maximum connections in the pool
            socket_timeout: Socket timeout in seconds
            **kwargs: Additional Queue initialization arguments

        Returns:
            Configured Queue instance

        Example:
            >>> queue = Queue.create_with_recommended_settings(
            ...     redis_url="redis://localhost:6379/0",
            ...     queue="emails",
            ...     max_connections=100,
            ...     enable_metrics=True
            ... )
        """
        client = RedisConnectionFactory.create_client(
            redis_url=redis_url,
            max_connections=max_connections,
            socket_timeout=socket_timeout
        )

        return cls(client=client, queue=queue, **kwargs)

    def push(self, name: str, dictObj: Dict[str, Any]) -> None:
        """Push a job to the queue."""
        if self.driver == 'redis':
            self.redisPush(name, dictObj)

    def listen(self) -> None:
        """Start listening to the queue."""
        if self.driver == 'redis':
            # Register shutdown handlers before starting
            if not self._shutdown_handlers_registered:
                self._register_shutdown_handlers()
            self.redisPop()

    def handler(self, f: Optional[Callable] = None) -> Union[Callable, Any]:
        """Decorator for registering job handlers."""
        def wrapper(f):
            self.ee._add_event_handler('queued', f, f)
        if f is None:
            return wrapper
        else:
            return wrapper(f)

    def _register_shutdown_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""
        def shutdown_handler(signum: int, frame: Any) -> None:
            signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else signum
            logger.info(f"Received {signal_name} signal, initiating graceful shutdown...")
            self._shutdown = True

            if self._processing_job:
                logger.info("Waiting for current job to finish...")
            else:
                logger.info("No job in progress, shutting down immediately")

        # Register handlers for SIGTERM and SIGINT
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        self._shutdown_handlers_registered = True
        logger.debug("Shutdown signal handlers registered (SIGINT, SIGTERM)")

    def _send_to_dead_letter_queue(self, job_data: Dict[str, Any], error: Exception, retry_count: int) -> None:
        """Send failed job to dead letter queue."""
        try:
            dead_letter_data = {
                'original_job': job_data,
                'error': {
                    'type': type(error).__name__,
                    'message': str(error),
                    'timestamp': time.time()
                },
                'retry_count': retry_count,
                'max_retries': self.max_retries,
                'failed_at': time.time(),
                'queue': self.queue
            }

            dead_letter_key = self._build_queue_key(self.dead_letter_queue)
            self.client.rpush(dead_letter_key, json.dumps(dead_letter_data))

            # Update statistics
            self._retry_stats['dead_letter_jobs'] += 1

            logger.warning(f"Job sent to dead letter queue '{self.dead_letter_queue}' after {retry_count} retries")
            logger.debug(f"Dead letter data: {dead_letter_data}")

        except Exception as dlq_error:
            logger.error(f"Failed to send job to dead letter queue: {dlq_error}")
            logger.error(f"Original job data: {job_data}")

    def _retry_job(self, job_data: Dict[str, Any], retry_count: int) -> None:
        """Retry a failed job with calculated delay."""
        try:
            # Calculate delay based on retry strategy
            delay = self._calculate_retry_delay(retry_count)

            # Add retry metadata to job data
            job_data['retry_delay'] = delay
            job_data['retry_attempt'] = retry_count
            job_data['retry_strategy'] = self.retry_strategy.value
            job_data['retry_timestamp'] = time.time()

            queue_key = self._build_queue_key()
            self.client.rpush(queue_key, json.dumps(job_data))

            # Update statistics
            self._retry_stats['total_retries'] += 1

            # Record retry in metrics
            if self.metrics:
                job_name = job_data.get('data', {}).get('commandName', 'UnknownJob')
                self.metrics.record_retry(job_name)

            logger.info(f"Job retried with {delay}s delay (attempt {retry_count}/{self.max_retries}, strategy: {self.retry_strategy.value})")

        except Exception as retry_error:
            logger.error(f"Failed to retry job: {retry_error}")
            self._retry_stats['failed_retries'] += 1

    def _update_horizon_metrics(self, job_id: str, job_name: str, status: str,
                               processing_time: Optional[float] = None,
                               error: Optional[Exception] = None) -> None:
        """
        Update Laravel Horizon metrics for job monitoring.

        Args:
            job_id: Job identifier
            job_name: Job class name
            status: Job status ('running', 'completed', 'failed')
            processing_time: Job processing time in seconds
            error: Exception if job failed
        """
        if not self.is_horizon or not self.horizon_metrics_enabled:
            return

        try:
            horizon_key = self._build_horizon_key(job_id)

            metrics = {
                'job_id': job_id,
                'job_name': job_name,
                'queue': self.queue,
                'status': status,
                'updated_at': int(time.time()),
            }

            if status == 'running':
                metrics['started_at'] = int(time.time())

            if status in ('completed', 'failed'):
                metrics['completed_at'] = int(time.time())
                if processing_time is not None:
                    metrics['duration'] = round(processing_time, 3)

            if status == 'failed' and error:
                metrics['exception'] = type(error).__name__
                metrics['exception_message'] = str(error)

            # Store in Redis hash
            self.client.hset(horizon_key, mapping=metrics)

            # Set TTL to prevent indefinite storage
            self.client.expire(horizon_key, self.horizon_ttl)

            logger.debug(f"Updated Horizon metrics for job {job_id}: {status}")

        except Exception as e:
            logger.warning(f"Failed to update Horizon metrics: {e}")
            # Don't fail job processing if Horizon update fails

    def get_dead_letter_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get jobs from dead letter queue."""
        try:
            dead_letter_key = self._build_queue_key(self.dead_letter_queue)
            jobs = self.client.lrange(dead_letter_key, 0, limit - 1)

            result = []
            for job in jobs:
                try:
                    job_data = json.loads(job)
                    result.append(job_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse dead letter job: {e}")

            return result

        except Exception as e:
            logger.error(f"Failed to get dead letter jobs: {e}")
            return []

    def reprocess_dead_letter_job(self, job_data: Dict[str, Any]) -> bool:
        """Reprocess a job from dead letter queue."""
        try:
            original_job = job_data.get('original_job', {})
            if not original_job:
                logger.error("No original job data found in dead letter job")
                return False

            # Clear retry count for reprocessing
            job_id = self._get_job_id(original_job)
            self._clear_retry_count(job_id)

            # Send back to main queue
            queue_key = self._build_queue_key()
            self.client.rpush(queue_key, json.dumps(original_job))

            logger.info(f"Dead letter job reprocessed: {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to reprocess dead letter job: {e}")
            return False

    def clear_dead_letter_queue(self) -> int:
        """Clear all jobs from dead letter queue."""
        try:
            dead_letter_key = self._build_queue_key(self.dead_letter_queue)
            count = self.client.llen(dead_letter_key)
            self.client.delete(dead_letter_key)

            logger.info(f"Cleared {count} jobs from dead letter queue")
            return count

        except Exception as e:
            logger.error(f"Failed to clear dead letter queue: {e}")
            return 0

    def get_metrics(self) -> Optional[Dict[str, Any]]:
        """Get comprehensive metrics."""
        if not self.metrics:
            return None
        return self.metrics.get_metrics()

    def reset_metrics(self) -> None:
        """Reset all metrics."""
        if self.metrics:
            self.metrics.reset_metrics()

    def get_recent_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent job history."""
        if not self.metrics:
            return []
        return self.metrics.get_recent_jobs(limit)

    def get_job_type_metrics(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Get metrics for specific job type."""
        if not self.metrics:
            return None

        metrics = self.metrics.get_metrics()
        return metrics.get('job_types', {}).get(job_name)

    def get_performance_summary(self) -> Optional[Dict[str, Any]]:
        """Get performance summary."""
        if not self.metrics:
            return None

        metrics = self.metrics.get_metrics()
        return {
            'general': metrics.get('general', {}),
            'performance': metrics.get('performance', {})
        }

    def redisPop(self) -> None:
        """Process jobs from Redis queue."""
        # Check if shutdown was requested
        if self._shutdown:
            logger.info("Shutdown requested, stopping worker loop")
            return

        try:
            result = self.client.blpop(self._build_queue_key(), 60)

            if result is None:
                # Timeout occurred, check shutdown flag before retrying
                if self._shutdown:
                    logger.info("Shutdown requested during timeout, stopping worker")
                    return
                logger.debug(f"Timeout waiting for job in queue '{self.queue}', retrying...")
                self.redisPop()
                return

            key, data = result

            # Mark that we're processing a job
            self._processing_job = True

            try:
                obj = json.loads(data)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error from queue '{self.queue}': {e}")
                logger.debug(f"Invalid data: {data}")
                self._processing_job = False
                self.redisPop()
                return

            try:
                command = obj['data']['command']
                # Handle both string and bytes for phpserialize.loads()
                if isinstance(command, str):
                    command = command.encode('utf-8')
                elif isinstance(command, bytes):
                    pass  # Already bytes
                else:
                    logger.error(f"Unexpected command type: {type(command)}")
                    self._processing_job = False
                    self.redisPop()
                    return

                raw = phpserialize.loads(command, object_hook=phpserialize.phpobject)
            except (KeyError, Exception) as e:
                logger.error(f"PHP object deserialization error: {e}")
                logger.error(f"Command type: {type(command)}")
                logger.error(f"Command content: {command[:100] if len(str(command)) > 100 else command}")
                logger.debug(f"Object data: {obj}")
                self._processing_job = False
                self.redisPop()
                return

            # Record job start time for metrics
            job_name = obj['data']['commandName']
            job_id = self._get_job_id(obj)
            start_time = time.time()

            if self.metrics:
                start_time = self.metrics.record_job_start(job_name)

            # Update Horizon metrics: job started
            self._update_horizon_metrics(job_id, job_name, 'running')

            try:
                self.ee.emit('queued', {'name': job_name, 'data': raw._asdict()})

                # Job processed successfully, clear retry count and update stats
                retry_count = self._get_retry_count(job_id)
                if retry_count > 0:
                    self._retry_stats['successful_retries'] += 1
                self._clear_retry_count(job_id)

                # Record successful job in metrics
                processing_time = time.time() - start_time
                if self.metrics:
                    self.metrics.record_job_success(job_name, start_time, processing_time)

                # Update Horizon metrics: job completed
                self._update_horizon_metrics(job_id, job_name, 'completed', processing_time)

            except Exception as e:
                logger.error(f"Error calling event handler: {e}")

                # Record failed job in metrics
                processing_time = time.time() - start_time
                if self.metrics:
                    self.metrics.record_job_failure(job_name, start_time, processing_time, e)

                # Update Horizon metrics: job failed
                self._update_horizon_metrics(job_id, job_name, 'failed', processing_time, e)

                # Handle job failure with retry/dead letter queue logic
                retry_count = self._increment_retry_count(job_id)

                if self._should_retry(job_id, e):
                    # Retry with calculated delay based on strategy
                    logger.warning(f"Job failed, retrying (attempt {retry_count}/{self.max_retries}, exception: {type(e).__name__})")
                    self._retry_job(obj, retry_count)
                else:
                    # Send to dead letter queue
                    logger.error(f"Job failed after {retry_count} attempts, sending to dead letter queue")
                    self._send_to_dead_letter_queue(obj, e, retry_count)
                    self._clear_retry_count(job_id)

            if self.is_queue_notify:
                try:
                    self.client.blpop(self._build_queue_key() + ':notify', 60)
                except (RedisConnectionError, RedisTimeoutError, RedisError) as e:
                    logger.warning(f"Error reading notify queue: {e}")
                    # Continue working, notify is not critical

            # Job processing completed
            self._processing_job = False

            # Check if shutdown was requested while processing
            if self._shutdown:
                logger.info("Shutdown requested, current job completed successfully")
                return

            self.redisPop()

        except RedisConnectionError as e:
            self._processing_job = False
            logger.error(f"Redis connection error: {e}")

            if self._shutdown:
                logger.info("Shutdown requested, not attempting reconnection")
                return

            logger.info("Waiting 5 seconds before reconnecting...")
            time.sleep(5)

            if self._shutdown:
                logger.info("Shutdown requested during reconnection wait")
                return

            try:
                self.redisPop()
            except Exception as retry_error:
                logger.critical(f"Failed to reconnect to Redis: {retry_error}")
                raise

        except RedisTimeoutError as e:
            self._processing_job = False
            logger.warning(f"Redis operation timeout: {e}")

            if not self._shutdown:
                self.redisPop()

        except RedisError as e:
            self._processing_job = False
            logger.error(f"Redis error: {e}")

            if self._shutdown:
                logger.info("Shutdown requested, not retrying after Redis error")
                return

            logger.info("Waiting 3 seconds before retry...")
            time.sleep(3)

            if not self._shutdown:
                self.redisPop()

        except KeyboardInterrupt:
            self._processing_job = False
            logger.info("Received interrupt signal, stopping worker...")
            self._shutdown = True
            raise

        except Exception as e:
            self._processing_job = False
            logger.error(f"Unexpected error processing queue: {e}", exc_info=True)

            if self._shutdown:
                logger.info("Shutdown requested, not retrying after unexpected error")
                return

            time.sleep(2)

            if not self._shutdown:
                self.redisPop()

    def redisPush(self, name: str, dictObj: Dict[str, Any], timeout: Optional[int] = None, delay: Optional[int] = None) -> None:
        """Push a job to Redis queue."""
        try:
            # Serialize PHP object
            try:
                command = phpserialize.dumps(phpserialize.phpobject(name, dictObj))
            except Exception as e:
                logger.error(f"PHP object serialization error '{name}': {e}")
                raise ValueError(f"Failed to serialize job data: {e}") from e

            # Prepare job data
            data = {
                "uuid": str(uuid.uuid4()),
                "job": 'Illuminate\\Queue\\CallQueuedHandler@call',
                "data": {
                    "commandName": name,
                    "command": command.decode("utf-8"),  # Decode for JSON
                },
                "timeout": timeout,
                "id": str(time.time()),
                "attempts": 0,
                "delay": delay,
                "maxExceptions": None,
            }

            if self.is_queue_notify == False:
                del data['delay']
                del data['maxExceptions']
                data.update({'displayName': name, 'maxTries': None, 'timeoutAt': None})

            # Serialize to JSON
            try:
                json_data = json.dumps(data)
            except (TypeError, ValueError) as e:
                logger.error(f"JSON serialization error: {e}")
                raise ValueError(f"Failed to create JSON payload: {e}") from e

            # Send to Redis
            queue_key = self._build_queue_key()
            try:
                self.client.rpush(queue_key, json_data)
                logger.debug(f"Job '{name}' successfully added to queue '{self.queue}'")
            except RedisConnectionError as e:
                logger.error(f"Redis connection error while pushing job: {e}")
                raise ConnectionError(f"Failed to connect to Redis: {e}") from e
            except RedisTimeoutError as e:
                logger.error(f"Timeout while pushing job to Redis: {e}")
                raise TimeoutError(f"Redis operation timeout exceeded: {e}") from e
            except RedisError as e:
                logger.error(f"Redis error while pushing job '{name}': {e}")
                raise RuntimeError(f"Redis operation error: {e}") from e

        except (ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            # Re-raise already handled errors
            raise
        except Exception as e:
            logger.error(f"Unexpected error while pushing job '{name}': {e}", exc_info=True)
            raise RuntimeError(f"Failed to add job to queue: {e}") from e
