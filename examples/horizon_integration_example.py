"""
Laravel Horizon Integration Example.

This example demonstrates:
1. Enabling Laravel Horizon support
2. Viewing Horizon metrics from Python
3. Monitoring job execution in Horizon dashboard
4. Configuring Horizon TTL and metrics
"""

from lara_queue import Queue, create_redis_client
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """
    Demonstrate Laravel Horizon integration.
    """

    # Create queue with Horizon support
    queue = Queue.create_with_recommended_settings(
        redis_url="redis://localhost:6379/0",
        queue="horizon_demo",

        # Horizon configuration
        is_horizon=True,  # Enable Horizon support
        horizon_metrics_enabled=True,  # Collect metrics
        horizon_ttl=86400,  # Keep metrics for 24 hours

        # Other settings
        enable_metrics=True,
        max_retries=3,
    )

    logger.info("Queue initialized with Laravel Horizon support")
    logger.info("Horizon metrics will be stored in Redis with 24-hour TTL")
    logger.info("")
    logger.info("You can view these metrics in Laravel Horizon dashboard:")
    logger.info("  - Job status (running, completed, failed)")
    logger.info("  - Processing time")
    logger.info("  - Error messages for failed jobs")
    logger.info("  - Queue name and job details")
    logger.info("")

    # Register job handlers
    @queue.handler
    def fast_job(job):
        """Fast job that completes quickly."""
        data = job.get('data', {})
        logger.info(f"Processing fast job: {data}")
        time.sleep(0.1)
        logger.info("Fast job completed")

    @queue.handler
    def slow_job(job):
        """Slow job that takes longer to complete."""
        data = job.get('data', {})
        logger.info(f"Processing slow job: {data}")
        time.sleep(2)
        logger.info("Slow job completed")

    @queue.handler
    def failing_job(job):
        """Job that fails to demonstrate error tracking."""
        data = job.get('data', {})
        logger.info(f"Processing failing job: {data}")

        # Simulate failure
        if data.get('should_fail', False):
            raise Exception("Intentional failure for Horizon demonstration")

        logger.info("Failing job completed without errors")

    # Demonstrate Horizon integration
    logger.info("=" * 60)
    logger.info("HORIZON INTEGRATION DEMO")
    logger.info("=" * 60)

    # Example: Push some test jobs
    logger.info("\n1. Pushing test jobs to queue...")

    # Fast job
    queue.push('App\\Jobs\\FastJob', {
        'message': 'This is a fast job',
        'timestamp': time.time()
    })
    logger.info("  ✓ Fast job pushed")

    # Slow job
    queue.push('App\\Jobs\\SlowJob', {
        'message': 'This is a slow job',
        'timestamp': time.time()
    })
    logger.info("  ✓ Slow job pushed")

    # Failing job
    queue.push('App\\Jobs\\FailingJob', {
        'message': 'This job will fail',
        'should_fail': True,
        'timestamp': time.time()
    })
    logger.info("  ✓ Failing job pushed")

    logger.info("\n2. Jobs have been pushed to queue")
    logger.info("   Horizon metrics will be updated as jobs are processed")

    logger.info("\n3. Starting worker to process jobs...")
    logger.info("   Watch the Horizon dashboard for real-time updates!")
    logger.info("   Press Ctrl+C after a few jobs are processed")
    logger.info("")

    # Start listening
    try:
        queue.listen()
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("HORIZON METRICS SUMMARY")
        logger.info("=" * 60)

        logger.info("\nHorizon metrics are stored in Redis with keys like:")
        logger.info("  laravel:horizon:jobs:{job_id}")
        logger.info("\nEach job stores the following data:")
        logger.info("  - job_id: Unique job identifier")
        logger.info("  - job_name: Job class name")
        logger.info("  - queue: Queue name")
        logger.info("  - status: running, completed, or failed")
        logger.info("  - started_at: Unix timestamp")
        logger.info("  - completed_at: Unix timestamp")
        logger.info("  - duration: Processing time in seconds")
        logger.info("  - exception: Exception type (if failed)")
        logger.info("  - exception_message: Error message (if failed)")

        if queue.metrics:
            metrics = queue.get_metrics()
            logger.info("\n" + "=" * 60)
            logger.info("LOCAL METRICS (non-Horizon):")
            logger.info(f"Total processed: {metrics['general']['total_processed']}")
            logger.info(f"Total successful: {metrics['general']['total_successful']}")
            logger.info(f"Total failed: {metrics['general']['total_failed']}")
            logger.info(f"Average time: {metrics['performance']['avg_processing_time']:.3f}s")

        logger.info("=" * 60)
        logger.info("\nTo view Horizon metrics in Laravel:")
        logger.info("  1. Install Laravel Horizon: composer require laravel/horizon")
        logger.info("  2. Visit: http://your-app/horizon")
        logger.info("  3. See all jobs processed by Python workers!")
        logger.info("=" * 60)


if __name__ == '__main__':
    main()
