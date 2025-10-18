"""
Base queue class with shared functionality for Queue and AsyncQueue.
"""
import json
import logging
import random
import signal
import time
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

from .retry_strategy import RetryStrategy

# Setup logger
logger = logging.getLogger(__name__)


class BaseQueue(ABC):
    """
    Abstract base class providing common functionality for Queue and AsyncQueue.

    This class contains all shared logic for retry mechanisms, dead letter queues,
    metrics, and configuration management. Subclasses must implement driver-specific
    methods (push, listen, etc.).
    """

    def __init__(
        self,
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
        max_retry_tracking: int = 10000,
    ) -> None:
        """
        Initialize base queue with common configuration.

        Args:
            queue: Queue name
            driver: Driver type (currently only 'redis')
            appname: Laravel application name
            prefix: Queue prefix
            is_queue_notify: Enable queue notifications
            is_horizon: Enable Laravel Horizon support
            horizon_metrics_enabled: Enable Horizon metrics collection
            horizon_ttl: TTL for Horizon metrics (seconds)
            dead_letter_queue: Dead letter queue name (default: {queue}:failed)
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
        self.driver = driver
        self.queue = queue
        self.appname = appname
        self.prefix = prefix
        self.is_queue_notify = is_queue_notify
        self.is_horizon = is_horizon
        self.horizon_metrics_enabled = horizon_metrics_enabled
        self.horizon_ttl = horizon_ttl

        # Graceful shutdown flags
        self._shutdown = False
        self._shutdown_handlers_registered = False

        # Dead letter queue configuration
        self.dead_letter_queue = dead_letter_queue or f"{queue}:failed"
        self.max_retries = max_retries

        # Retry count tracking with TTL to prevent memory leaks
        # Format: OrderedDict[job_id, (retry_count, last_updated_timestamp)]
        self._job_retry_count: OrderedDict[str, Tuple[int, float]] = OrderedDict()
        self._retry_tracking_ttl = retry_tracking_ttl
        self._max_retry_tracking = max_retry_tracking
        self._cleanup_counter = 0  # For periodic cleanup

        # Retry configuration
        self.retry_strategy = retry_strategy
        self.retry_delay = retry_delay
        self.retry_max_delay = retry_max_delay
        self.retry_jitter = retry_jitter
        self.retry_backoff_multiplier = retry_backoff_multiplier
        self.retry_custom_function = retry_custom_function
        self.retry_exceptions = retry_exceptions or [Exception]

        # Retry statistics
        self._retry_stats = {
            'total_retries': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'dead_letter_jobs': 0,
        }

        # Metrics configuration (subclasses will initialize metrics collector)
        self.enable_metrics = enable_metrics
        self.metrics = None  # Set by subclasses

    def _get_job_id(self, job_data: Dict[str, Any]) -> str:
        """
        Generate or extract job ID for retry tracking.

        Args:
            job_data: Job data dictionary

        Returns:
            Job ID string
        """
        return job_data.get('uuid', str(uuid.uuid4()))

    def _cleanup_old_retry_counts(self) -> None:
        """
        Clean up old retry count entries based on TTL.

        This method removes entries that haven't been updated within the TTL period,
        preventing memory leaks in long-running workers.
        """
        current_time = time.time()
        expired_jobs = [
            job_id
            for job_id, (_, timestamp) in self._job_retry_count.items()
            if current_time - timestamp > self._retry_tracking_ttl
        ]

        for job_id in expired_jobs:
            del self._job_retry_count[job_id]

        if expired_jobs:
            logger.debug(f"Cleaned up {len(expired_jobs)} expired retry tracking entries")

    def _maybe_cleanup_retry_counts(self) -> None:
        """
        Conditionally trigger cleanup based on size and operation count.

        Cleanup is triggered when:
        - Every 1000 operations, OR
        - Retry tracking dictionary exceeds max_retry_tracking size
        """
        self._cleanup_counter += 1

        should_cleanup = (
            self._cleanup_counter % 1000 == 0
            or len(self._job_retry_count) > self._max_retry_tracking
        )

        if should_cleanup:
            self._cleanup_old_retry_counts()

    def _increment_retry_count(self, job_id: str) -> int:
        """
        Increment retry count for a job with TTL tracking.

        Args:
            job_id: Job identifier

        Returns:
            Updated retry count
        """
        # Periodic cleanup to prevent memory leaks
        self._maybe_cleanup_retry_counts()

        current_time = time.time()

        if job_id not in self._job_retry_count:
            self._job_retry_count[job_id] = (0, current_time)

        count, _ = self._job_retry_count[job_id]
        count += 1

        # Update with new count and timestamp
        self._job_retry_count[job_id] = (count, current_time)

        # Move to end in OrderedDict (LRU behavior)
        self._job_retry_count.move_to_end(job_id)

        return count

    def _get_retry_count(self, job_id: str) -> int:
        """
        Get current retry count for a job.

        Args:
            job_id: Job identifier

        Returns:
            Current retry count
        """
        if job_id in self._job_retry_count:
            count, _ = self._job_retry_count[job_id]
            return count
        return 0

    def _clear_retry_count(self, job_id: str) -> None:
        """
        Clear retry count for a job (on success).

        Args:
            job_id: Job identifier
        """
        if job_id in self._job_retry_count:
            del self._job_retry_count[job_id]

    def _should_retry(self, job_id: str, exception: Exception) -> bool:
        """
        Check if job should be retried based on retry count and exception type.

        Args:
            job_id: Job identifier
            exception: Exception that caused failure

        Returns:
            True if job should be retried
        """
        retry_count = self._get_retry_count(job_id)

        # Check retry count
        if retry_count >= self.max_retries:
            return False

        # Check exception type
        if not self._is_retryable_exception(exception):
            return False

        return True

    def _is_retryable_exception(self, exception: Exception) -> bool:
        """
        Check if exception type is retryable.

        Args:
            exception: Exception to check

        Returns:
            True if exception is retryable
        """
        return any(isinstance(exception, exc_type) for exc_type in self.retry_exceptions)

    def _calculate_retry_delay(self, retry_count: int) -> int:
        """
        Calculate delay for retry based on strategy.

        Args:
            retry_count: Current retry attempt number

        Returns:
            Delay in seconds
        """
        if self.retry_strategy == RetryStrategy.CUSTOM and self.retry_custom_function:
            delay = self.retry_custom_function(retry_count)
        elif self.retry_strategy == RetryStrategy.EXPONENTIAL:
            delay = int(self.retry_delay * (self.retry_backoff_multiplier ** (retry_count - 1)))
        elif self.retry_strategy == RetryStrategy.LINEAR:
            delay = self.retry_delay * retry_count
        elif self.retry_strategy == RetryStrategy.FIXED:
            delay = self.retry_delay
        else:
            delay = self.retry_delay

        # Limit by maximum delay
        delay = min(delay, self.retry_max_delay)

        # Add jitter to avoid thundering herd
        if self.retry_jitter:
            jitter = random.uniform(0.1, 0.3) * delay
            delay = int(delay + jitter)

        return max(1, delay)  # Minimum 1 second

    def get_retry_statistics(self) -> Dict[str, Any]:
        """
        Get retry statistics.

        Returns:
            Dictionary with retry statistics and configuration
        """
        return {
            'total_retries': self._retry_stats['total_retries'],
            'successful_retries': self._retry_stats['successful_retries'],
            'failed_retries': self._retry_stats['failed_retries'],
            'dead_letter_jobs': self._retry_stats['dead_letter_jobs'],
            'success_rate': (
                self._retry_stats['successful_retries'] / max(1, self._retry_stats['total_retries'])
            ) * 100,
            'current_retry_tracking': len(self._job_retry_count),
            'current_retry_config': {
                'strategy': self.retry_strategy.value,
                'max_retries': self.max_retries,
                'delay': self.retry_delay,
                'max_delay': self.retry_max_delay,
                'jitter': self.retry_jitter,
                'backoff_multiplier': self.retry_backoff_multiplier,
                'retryable_exceptions': [exc.__name__ for exc in self.retry_exceptions],
                'tracking_ttl': self._retry_tracking_ttl,
                'max_tracking_entries': self._max_retry_tracking,
            },
        }

    def reset_retry_statistics(self) -> None:
        """Reset retry statistics."""
        self._retry_stats = {
            'total_retries': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'dead_letter_jobs': 0,
        }
        logger.info("Retry statistics reset")

    def update_retry_config(
        self,
        max_retries: Optional[int] = None,
        retry_strategy: Optional[RetryStrategy] = None,
        retry_delay: Optional[int] = None,
        retry_max_delay: Optional[int] = None,
        retry_jitter: Optional[bool] = None,
        retry_backoff_multiplier: Optional[float] = None,
        retry_custom_function: Optional[Callable[[int], int]] = None,
        retry_exceptions: Optional[List[type]] = None,
    ) -> None:
        """
        Update retry configuration at runtime.

        Args:
            max_retries: Maximum retry attempts
            retry_strategy: Retry strategy
            retry_delay: Initial retry delay
            retry_max_delay: Maximum retry delay
            retry_jitter: Enable jitter
            retry_backoff_multiplier: Backoff multiplier
            retry_custom_function: Custom retry function
            retry_exceptions: Retryable exception types
        """
        if max_retries is not None:
            self.max_retries = max_retries
        if retry_strategy is not None:
            self.retry_strategy = retry_strategy
        if retry_delay is not None:
            self.retry_delay = retry_delay
        if retry_max_delay is not None:
            self.retry_max_delay = retry_max_delay
        if retry_jitter is not None:
            self.retry_jitter = retry_jitter
        if retry_backoff_multiplier is not None:
            self.retry_backoff_multiplier = retry_backoff_multiplier
        if retry_custom_function is not None:
            self.retry_custom_function = retry_custom_function
        if retry_exceptions is not None:
            self.retry_exceptions = retry_exceptions

        logger.info(f"Retry configuration updated: {self.get_retry_statistics()['current_retry_config']}")

    def shutdown(self) -> None:
        """Trigger graceful shutdown manually."""
        logger.info("Manual shutdown requested")
        self._shutdown = True

    def _build_queue_key(self, queue_name: Optional[str] = None) -> str:
        """
        Build Redis queue key.

        Args:
            queue_name: Queue name (uses self.queue if not provided)

        Returns:
            Full Redis key
        """
        queue = queue_name or self.queue
        return f"{self.appname}{self.prefix}queues:{queue}"

    def _build_horizon_key(self, job_id: str, suffix: str = "") -> str:
        """
        Build Laravel Horizon Redis key.

        Args:
            job_id: Job identifier
            suffix: Optional suffix for the key

        Returns:
            Full Horizon Redis key
        """
        base_key = f"{self.appname}:horizon:jobs:{job_id}"
        if suffix:
            return f"{base_key}:{suffix}"
        return base_key

    # Abstract methods that must be implemented by subclasses
    @abstractmethod
    def push(self, name: str, dictObj: Dict[str, Any]) -> None:
        """
        Push a job to the queue.

        Args:
            name: Job class name
            dictObj: Job data dictionary
        """
        pass

    @abstractmethod
    def listen(self) -> None:
        """Start listening to the queue and processing jobs."""
        pass

    @abstractmethod
    def handler(self, f: Optional[Callable] = None):
        """
        Decorator for registering job handlers.

        Args:
            f: Handler function
        """
        pass
