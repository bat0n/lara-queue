"""
Production-ready async setup example with optimized Redis connection pooling.

This example demonstrates:
1. Using async factory methods for production-ready configuration
2. Optimized async Redis connection pooling
3. High-concurrency job processing
4. Laravel Horizon integration
5. Comprehensive error handling and metrics
"""

import asyncio
import logging
from lara_queue import AsyncQueue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def main():
    """
    Production-ready async queue setup with recommended settings.
    """

    # Using the async factory method (RECOMMENDED)
    queue = await AsyncQueue.create_with_recommended_settings(
        redis_url="redis://localhost:6379/0",
        queue="async_high_priority",
        max_connections=150,  # Higher pool for async workloads
        socket_timeout=60,

        # Async-specific configuration
        max_concurrent_jobs=50,  # Process up to 50 jobs concurrently

        # Queue configuration
        enable_metrics=True,
        is_horizon=True,  # Enable Laravel Horizon support
        horizon_metrics_enabled=True,
        horizon_ttl=86400,  # Keep Horizon metrics for 24 hours

        # Retry configuration
        max_retries=5,
        retry_delay=5,
        retry_max_delay=300,
        retry_jitter=True,

        # Memory leak prevention
        retry_tracking_ttl=3600,  # Clean up old retry tracking after 1 hour
        max_retry_tracking=10000,  # Maximum retry tracking entries
    )

    logger.info("Async queue initialized with production-ready settings")
    logger.info(f"Connection pool: max_connections=150, socket_timeout=60s")
    logger.info(f"Max concurrent jobs: 50")
    logger.info(f"Horizon support: enabled")
    logger.info(f"Metrics collection: enabled")

    # Register async job handlers
    @queue.handler
    async def process_email(job):
        """Process email job asynchronously."""
        data = job.get('data', {})
        email = data.get('email')
        subject = data.get('subject')

        logger.info(f"Processing email: to={email}, subject={subject}")

        # Simulate async email sending
        await asyncio.sleep(0.1)  # Simulate I/O operation
        # await send_email_async(email, subject, body)

        logger.info(f"Email sent successfully to {email}")

    @queue.handler
    async def process_notification(job):
        """Process notification job asynchronously."""
        data = job.get('data', {})
        user_id = data.get('user_id')
        message = data.get('message')

        logger.info(f"Processing notification: user={user_id}, message={message}")

        # Simulate async notification sending
        await asyncio.sleep(0.05)  # Simulate I/O operation
        # await send_notification_async(user_id, message)

        logger.info(f"Notification sent successfully to user {user_id}")

    @queue.handler
    async def process_heavy_task(job):
        """Process heavy computational task."""
        data = job.get('data', {})
        task_id = data.get('task_id')

        logger.info(f"Processing heavy task: {task_id}")

        # Simulate heavy async processing
        await asyncio.sleep(1)
        # await process_data_async(task_id)

        logger.info(f"Heavy task completed: {task_id}")

    # Start listening for jobs
    logger.info(f"Starting async worker for queue '{queue.queue}'...")
    logger.info("Press Ctrl+C to gracefully shutdown")

    try:
        await queue.listen()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received, stopping async worker...")

        # Print final statistics
        if queue.metrics:
            metrics = await queue.get_metrics()
            logger.info("=" * 60)
            logger.info("FINAL METRICS:")
            logger.info(f"Total processed: {metrics['general']['total_processed']}")
            logger.info(f"Total successful: {metrics['general']['total_successful']}")
            logger.info(f"Total failed: {metrics['general']['total_failed']}")
            logger.info(f"Average processing time: {metrics['performance']['avg_processing_time']:.3f}s")
            logger.info(f"Throughput: {metrics['performance']['throughput_per_second']:.2f} jobs/sec")
            logger.info("=" * 60)

        retry_stats = queue.get_retry_statistics()
        logger.info("RETRY STATISTICS:")
        logger.info(f"Total retries: {retry_stats['total_retries']}")
        logger.info(f"Successful retries: {retry_stats['successful_retries']}")
        logger.info(f"Dead letter jobs: {retry_stats['dead_letter_jobs']}")
        logger.info(f"Retry success rate: {retry_stats['success_rate']:.2f}%")
        logger.info("=" * 60)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Async worker stopped")
