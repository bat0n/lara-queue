"""
Production-ready setup example with optimized Redis connection pooling.

This example demonstrates:
1. Using factory methods for production-ready configuration
2. Optimized Redis connection pooling
3. Laravel Horizon integration
4. Comprehensive error handling and metrics
"""

from lara_queue import Queue
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """
    Production-ready queue setup with recommended settings.
    """

    # Method 1: Using the factory method (RECOMMENDED)
    queue = Queue.create_with_recommended_settings(
        redis_url="redis://localhost:6379/0",
        queue="high_priority",
        max_connections=100,  # Optimized connection pool
        socket_timeout=60,

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

    logger.info("Queue initialized with production-ready settings")
    logger.info(f"Connection pool: max_connections=100, socket_timeout=60s")
    logger.info(f"Horizon support: enabled")
    logger.info(f"Metrics collection: enabled")

    # Register job handlers
    @queue.handler
    def process_email(job):
        """Process email job."""
        data = job.get('data', {})
        email = data.get('email')
        subject = data.get('subject')

        logger.info(f"Processing email: to={email}, subject={subject}")

        # Simulate email sending
        # send_email(email, subject, body)

        logger.info(f"Email sent successfully to {email}")

    @queue.handler
    def process_notification(job):
        """Process notification job."""
        data = job.get('data', {})
        user_id = data.get('user_id')
        message = data.get('message')

        logger.info(f"Processing notification: user={user_id}, message={message}")

        # Simulate notification sending
        # send_notification(user_id, message)

        logger.info(f"Notification sent successfully to user {user_id}")

    # Start listening for jobs
    logger.info(f"Starting worker for queue '{queue.queue}'...")
    logger.info("Press Ctrl+C to gracefully shutdown")

    try:
        queue.listen()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received, stopping worker...")

        # Print final statistics
        if queue.metrics:
            metrics = queue.get_metrics()
            logger.info("=" * 60)
            logger.info("FINAL METRICS:")
            logger.info(f"Total processed: {metrics['general']['total_processed']}")
            logger.info(f"Total successful: {metrics['general']['total_successful']}")
            logger.info(f"Total failed: {metrics['general']['total_failed']}")
            logger.info(f"Average processing time: {metrics['performance']['avg_processing_time']:.3f}s")
            logger.info("=" * 60)

        retry_stats = queue.get_retry_statistics()
        logger.info("RETRY STATISTICS:")
        logger.info(f"Total retries: {retry_stats['total_retries']}")
        logger.info(f"Successful retries: {retry_stats['successful_retries']}")
        logger.info(f"Dead letter jobs: {retry_stats['dead_letter_jobs']}")
        logger.info(f"Retry success rate: {retry_stats['success_rate']:.2f}%")
        logger.info("=" * 60)


if __name__ == '__main__':
    main()
