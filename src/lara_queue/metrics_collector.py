"""
Metrics collection implementation for LaraQueue.

This module provides metrics collection and performance monitoring for queue operations.
"""

import time
from typing import Dict, List, Any, Optional
from collections import defaultdict, deque


class MetricsCollector:
    """Collects and manages metrics for queue operations."""
    
    def __init__(self, max_history_size: int = 1000):
        """
        Initialize metrics collector.
        
        Args:
            max_history_size: Maximum number of recent jobs to keep in history
        """
        self.max_history_size = max_history_size
        
        # General counters (for backward compatibility)
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.total_retries = 0
        
        # General metrics
        self.metrics = {
            'total_processed': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_retries': 0,
            'start_time': time.time()
        }
        
        # Job type specific metrics
        self.job_type_metrics = defaultdict(lambda: {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'retries': 0,
            'total_time': 0.0,
            'min_time': float('inf'),
            'max_time': 0.0
        })
        
        # Backward compatibility attributes
        self.job_type_counts = defaultdict(int)
        self.job_type_success = defaultdict(int)
        self.job_type_failed = defaultdict(int)
        self.job_type_processing_times = defaultdict(list)
        
        # Error tracking
        self.error_counts = defaultdict(int)
        self.error_types = defaultdict(int)
        self.error_details = defaultdict(list)
        
        # Recent jobs history
        self.recent_jobs = deque(maxlen=max_history_size)
        self.job_history = deque(maxlen=max_history_size)
        self.processing_times = deque(maxlen=max_history_size)
        
        # Performance tracking
        self.performance_stats = {
            'avg_processing_time': 0.0,
            'total_processing_time': 0.0,
            'throughput_per_minute': 0.0
        }
        
        # Performance metrics
        self.start_time = time.time()
        self.last_reset_time = time.time()
    
    def record_job_start(self, job_name: str) -> float:
        """
        Record the start of a job processing.
        
        Args:
            job_name: Name of the job being processed
            
        Returns:
            Start timestamp
        """
        return time.time()
    
    def record_job_success(self, job_name: str, start_time: float, processing_time: float):
        """
        Record successful job completion.
        
        Args:
            job_name: Name of the job
            start_time: Start timestamp from record_job_start
            processing_time: Processing time in seconds
        """
        # Update general counters
        self.total_processed += 1
        self.total_successful += 1
        self.metrics['total_processed'] += 1
        self.metrics['total_successful'] += 1
        
        # Update job type metrics
        self.job_type_counts[job_name] += 1
        self.job_type_success[job_name] += 1
        self.job_type_processing_times[job_name].append(processing_time)
        
        # Update new metrics
        job_metrics = self.job_type_metrics[job_name]
        job_metrics['processed'] += 1
        job_metrics['successful'] += 1
        job_metrics['total_time'] += processing_time
        job_metrics['min_time'] = min(job_metrics['min_time'], processing_time)
        job_metrics['max_time'] = max(job_metrics['max_time'], processing_time)
        
        # Update performance stats
        self.processing_times.append(processing_time)
        self.performance_stats['total_processing_time'] += processing_time
        if self.total_processed > 0:
            self.performance_stats['avg_processing_time'] = (
                self.performance_stats['total_processing_time'] / self.total_processed
            )
        
        # Record in job history
        self.job_history.append({
            'name': job_name,
            'success': True,
            'processing_time': processing_time,
            'timestamp': start_time
        })
    
    def record_job_failure(self, job_name: str, start_time: float, processing_time: float, error: Exception):
        """
        Record job failure.
        
        Args:
            job_name: Name of the job
            start_time: Start timestamp from record_job_start
            processing_time: Processing time in seconds
            error: The exception that caused the failure
        """
        error_type = type(error).__name__
        
        # Update general counters
        self.total_processed += 1
        self.total_failed += 1
        self.metrics['total_processed'] += 1
        self.metrics['total_failed'] += 1
        
        # Update job type metrics
        self.job_type_counts[job_name] += 1
        self.job_type_failed[job_name] += 1
        self.job_type_processing_times[job_name].append(processing_time)
        
        # Update new metrics
        job_metrics = self.job_type_metrics[job_name]
        job_metrics['processed'] += 1
        job_metrics['failed'] += 1
        job_metrics['total_time'] += processing_time
        job_metrics['min_time'] = min(job_metrics['min_time'], processing_time)
        job_metrics['max_time'] = max(job_metrics['max_time'], processing_time)
        
        # Update error tracking
        self.error_counts[error_type] += 1
        self.error_types[str(error)] += 1
        self.error_details[error_type].append({
            'job_name': job_name,
            'timestamp': time.time(),
            'message': str(error)
        })
        
        # Update performance stats
        self.processing_times.append(processing_time)
        self.performance_stats['total_processing_time'] += processing_time
        if self.total_processed > 0:
            self.performance_stats['avg_processing_time'] = (
                self.performance_stats['total_processing_time'] / self.total_processed
            )
        
        # Record in job history
        self.job_history.append({
            'name': job_name,
            'success': False,
            'processing_time': processing_time,
            'timestamp': start_time,
            'error': error_type
        })
    
    def record_retry(self, job_name: str):
        """
        Record a retry attempt.
        
        Args:
            job_name: Name of the job being retried
        """
        self.total_retries += 1
        self.metrics['total_retries'] += 1
        
        # Update job type metrics
        self.job_type_metrics[job_name]['retries'] += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive metrics.
        
        Returns:
            Dictionary containing all metrics
        """
        current_time = time.time()
        uptime = current_time - self.start_time
        time_since_reset = current_time - self.last_reset_time
        
        # Calculate averages
        avg_processing_time = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        min_processing_time = min(self.processing_times) if self.processing_times else 0
        max_processing_time = max(self.processing_times) if self.processing_times else 0
        
        # Throughput (jobs per second)
        throughput = self.total_processed / time_since_reset if time_since_reset > 0 else 0
        
        # Success rate
        success_rate = (self.total_successful / self.total_processed * 100) if self.total_processed > 0 else 0
        
        # Metrics by job types
        job_type_metrics = {}
        for job_name in self.job_type_counts:
            total_jobs = self.job_type_counts[job_name]
            successful_jobs = self.job_type_success[job_name]
            failed_jobs = self.job_type_failed[job_name]
            processing_times = self.job_type_processing_times[job_name]
            
            job_type_metrics[job_name] = {
                'total': total_jobs,
                'successful': successful_jobs,
                'failed': failed_jobs,
                'success_rate': (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0,
                'avg_processing_time': sum(processing_times) / len(processing_times) if processing_times else 0,
                'min_processing_time': min(processing_times) if processing_times else 0,
                'max_processing_time': max(processing_times) if processing_times else 0
            }
        
        return {
            'general': {
                'total_processed': self.total_processed,
                'total_successful': self.total_successful,
                'total_failed': self.total_failed,
                'total_retries': self.total_retries,
                'success_rate': success_rate,
                'uptime_seconds': uptime,
                'time_since_reset': time_since_reset
            },
            'performance': {
                'avg_processing_time': avg_processing_time,
                'min_processing_time': min_processing_time,
                'max_processing_time': max_processing_time,
                'throughput_per_second': throughput,
                'history_size': len(self.processing_times)
            },
            'job_types': job_type_metrics,
            'errors': {
                'error_counts': dict(self.error_counts),
                'error_types': dict(self.error_types),
                'error_details': dict(self.error_details)
            }
        }
    
    def get_job_type_metrics(self, job_name: str) -> Dict[str, Any]:
        """
        Get metrics for a specific job type.
        
        Args:
            job_name: Name of the job type
            
        Returns:
            Dictionary containing job type metrics
        """
        if job_name not in self.job_type_counts:
            return {}
        
        total_jobs = self.job_type_counts[job_name]
        successful_jobs = self.job_type_success[job_name]
        failed_jobs = self.job_type_failed[job_name]
        processing_times = self.job_type_processing_times[job_name]
        
        return {
            'total': total_jobs,
            'successful': successful_jobs,
            'failed': failed_jobs,
            'success_rate': (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0,
            'avg_processing_time': sum(processing_times) / len(processing_times) if processing_times else 0,
            'min_processing_time': min(processing_times) if processing_times else 0,
            'max_processing_time': max(processing_times) if processing_times else 0
        }
    
    def get_recent_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent job records.
        
        Args:
            limit: Maximum number of recent jobs to return
            
        Returns:
            List of recent job records
        """
        return list(self.job_history)[-limit:]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get performance summary.
        
        Returns:
            Dictionary containing performance summary
        """
        uptime = time.time() - self.start_time
        
        return {
            'total_jobs': self.total_processed,
            'success_rate': (
                (self.total_successful / max(1, self.total_processed)) * 100
            ),
            'avg_processing_time': (
                self.performance_stats['total_processing_time'] / max(1, self.total_processed)
            ),
            'throughput_per_minute': (
                self.total_processed / max(1, uptime / 60)
            ),
            'uptime_hours': uptime / 3600,
            'total_retries': self.total_retries
        }
    
    def reset_metrics(self):
        """Reset all metrics to initial state."""
        # Reset general counters
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.total_retries = 0
        
        # Reset metrics
        self.metrics = {
            'total_processed': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_retries': 0,
            'start_time': time.time()
        }
        
        # Reset job type metrics
        self.job_type_counts.clear()
        self.job_type_success.clear()
        self.job_type_failed.clear()
        self.job_type_processing_times.clear()
        self.job_type_metrics.clear()
        
        # Reset error tracking
        self.error_counts.clear()
        self.error_types.clear()
        self.error_details.clear()
        
        # Reset history
        self.recent_jobs.clear()
        self.job_history.clear()
        self.processing_times.clear()
        
        # Reset performance stats
        self.performance_stats = {
            'avg_processing_time': 0.0,
            'total_processing_time': 0.0,
            'throughput_per_minute': 0.0
        }
        
        # Reset timestamps
        self.last_reset_time = time.time()
