"""
Retry strategy implementation for LaraQueue.

This module provides retry strategies and configuration for handling failed jobs.
"""

import enum
import random
import time
from typing import Callable, List, Type, Union, Any, Dict


class RetryStrategy(enum.Enum):
    """Enumeration of available retry strategies."""
    
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"
    CUSTOM = "custom"


class RetryConfig:
    """Configuration class for retry behavior."""
    
    def __init__(
        self,
        max_retries: int = 3,
        retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
        retry_delay: int = 1,
        retry_max_delay: int = 300,
        retry_jitter: bool = True,
        retry_backoff_multiplier: float = 2.0,
        retry_custom_function: Callable[[int], int] = None,
        retry_exceptions: List[Type[Exception]] = None
    ):
        """
        Initialize retry configuration.
        
        Args:
            max_retries: Maximum number of retry attempts
            retry_strategy: Strategy for calculating retry delays
            retry_delay: Base delay in seconds for retries
            retry_max_delay: Maximum delay in seconds
            retry_jitter: Whether to add random jitter to delays
            retry_backoff_multiplier: Multiplier for exponential/linear strategies
            retry_custom_function: Custom function for calculating delays
            retry_exceptions: List of exception types that should be retried
        """
        self.max_retries = max_retries
        self.retry_strategy = retry_strategy
        self.retry_delay = retry_delay
        self.retry_max_delay = retry_max_delay
        self.retry_jitter = retry_jitter
        self.retry_backoff_multiplier = retry_backoff_multiplier
        self.retry_custom_function = retry_custom_function
        self.retry_exceptions = retry_exceptions or [Exception]
    
    def calculate_delay(self, attempt: int) -> int:
        """
        Calculate retry delay based on strategy and attempt number.
        
        Args:
            attempt: Current attempt number (1-based)
            
        Returns:
            Delay in seconds
        """
        if self.retry_strategy == RetryStrategy.EXPONENTIAL:
            delay = self.retry_delay * (self.retry_backoff_multiplier ** (attempt - 1))
        elif self.retry_strategy == RetryStrategy.LINEAR:
            delay = self.retry_delay * attempt
        elif self.retry_strategy == RetryStrategy.FIXED:
            delay = self.retry_delay
        elif self.retry_strategy == RetryStrategy.CUSTOM and self.retry_custom_function:
            delay = self.retry_custom_function(attempt)
        else:
            delay = self.retry_delay
        
        # Apply jitter if enabled
        if self.retry_jitter and delay > 0:
            jitter_factor = random.uniform(0.1, 0.3)  # 10-30% jitter
            delay = int(delay * (1 + jitter_factor))
        
        # Apply limits
        delay = max(1, min(delay, self.retry_max_delay))
        
        return delay
    
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """
        Determine if a job should be retried.
        
        Args:
            attempt: Current attempt number (1-based)
            exception: The exception that occurred
            
        Returns:
            True if job should be retried, False otherwise
        """
        if attempt > self.max_retries:
            return False
        
        # Check if exception is retryable
        return any(isinstance(exception, exc_type) for exc_type in self.retry_exceptions)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'max_retries': self.max_retries,
            'retry_strategy': self.retry_strategy.value,
            'retry_delay': self.retry_delay,
            'retry_max_delay': self.retry_max_delay,
            'retry_jitter': self.retry_jitter,
            'retry_backoff_multiplier': self.retry_backoff_multiplier,
            'retry_exceptions': [exc.__name__ for exc in self.retry_exceptions]
        }
