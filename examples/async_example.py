"""
Example of using LaraQueue async queue for high loads.

This example demonstrates:
- Asynchronous job processing
- High performance with concurrent jobs
- Graceful shutdown
- Metrics and monitoring
- Retry mechanisms
"""

import asyncio
import logging
import time
from typing import Dict, Any

import aioredis
from lara_queue import AsyncQueue, RetryStrategy

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def create_redis_client() -> aioredis.Redis:
    """Создает асинхронный Redis клиент."""
    return await aioredis.from_url("redis://localhost:6379")


async def basic_async_example():
    """Базовый пример асинхронной очереди."""
    logger.info("=== Базовый асинхронный пример ===")
    
    # Создаем Redis клиент
    redis_client = await create_redis_client()
    
    # Создаем асинхронную очередь
    queue = AsyncQueue(
        client=redis_client,
        queue='async_worker',
        max_concurrent_jobs=5,  # Обрабатываем до 5 задач одновременно
        enable_metrics=True
    )
    
    # Обработчик задач
    @queue.handler
    async def process_email(data: Dict[str, Any]) -> None:
        """Асинхронно обрабатывает email задачи."""
        job_name = data.get('name', 'UnknownJob')
        job_data = data.get('data', {})
        
        logger.info(f"Обрабатываем email: {job_data.get('subject', 'No subject')}")
        
        # Имитируем асинхронную работу (отправка email)
        await asyncio.sleep(1)
        
        logger.info(f"Email отправлен: {job_data.get('to', 'unknown@example.com')}")
    
    # Добавляем задачи в очередь
    for i in range(10):
        await queue.push('App\\Jobs\\EmailJob', {
            'to': f'user{i}@example.com',
            'subject': f'Test Email {i}',
            'body': f'This is test email number {i}'
        })
    
    logger.info("Добавлено 10 задач в очередь")
    
    # Запускаем обработку (в реальном приложении это будет в отдельном процессе)
    logger.info("Запускаем обработку задач...")
    
    # Обрабатываем задачи в течение 15 секунд
    try:
        await asyncio.wait_for(queue.listen(), timeout=15.0)
    except asyncio.TimeoutError:
        logger.info("Время обработки истекло")
    
    # Показываем метрики
    metrics = queue.get_metrics()
    if metrics:
        logger.info(f"Обработано задач: {metrics['general']['total_processed']}")
        logger.info(f"Успешно: {metrics['general']['total_successful']}")
        logger.info(f"Неудачно: {metrics['general']['total_failed']}")
        logger.info(f"Throughput: {metrics['performance']['throughput_per_second']:.2f} задач/сек")
    
    await redis_client.close()


async def high_performance_example():
    """Пример высокой производительности с множественными задачами."""
    logger.info("=== Пример высокой производительности ===")
    
    redis_client = await create_redis_client()
    
    # Создаем очередь с высокой производительностью
    queue = AsyncQueue(
        client=redis_client,
        queue='high_perf_worker',
        max_concurrent_jobs=20,  # 20 одновременных задач
        enable_metrics=True,
        metrics_history_size=5000
    )
    
    # Быстрый обработчик
    @queue.handler
    async def fast_processor(data: Dict[str, Any]) -> None:
        """Быстрый обработчик задач."""
        job_data = data.get('data', {})
        task_id = job_data.get('task_id', 'unknown')
        
        # Имитируем быструю обработку
        await asyncio.sleep(0.1)
        
        logger.debug(f"Обработана задача {task_id}")
    
    # Добавляем много задач
    start_time = time.time()
    task_count = 100
    
    for i in range(task_count):
        await queue.push('App\\Jobs\\FastJob', {
            'task_id': i,
            'data': f'Task data {i}',
            'priority': i % 3
        })
    
    add_time = time.time() - start_time
    logger.info(f"Добавлено {task_count} задач за {add_time:.2f} секунд")
    
    # Обрабатываем задачи
    process_start = time.time()
    try:
        await asyncio.wait_for(queue.listen(), timeout=30.0)
    except asyncio.TimeoutError:
        pass
    
    process_time = time.time() - process_start
    
    # Показываем результаты
    metrics = queue.get_metrics()
    if metrics:
        logger.info(f"Время обработки: {process_time:.2f} секунд")
        logger.info(f"Обработано: {metrics['general']['total_processed']} задач")
        logger.info(f"Throughput: {metrics['performance']['throughput_per_second']:.2f} задач/сек")
        logger.info(f"Среднее время обработки: {metrics['performance']['avg_processing_time']:.3f} сек")
    
    await redis_client.close()


async def retry_and_error_handling_example():
    """Пример обработки ошибок и retry механизмов."""
    logger.info("=== Пример retry и обработки ошибок ===")
    
    redis_client = await create_redis_client()
    
    # Создаем очередь с retry стратегией
    queue = AsyncQueue(
        client=redis_client,
        queue='retry_worker',
        max_retries=3,
        retry_strategy=RetryStrategy.EXPONENTIAL,
        retry_delay=1,
        retry_max_delay=10,
        retry_jitter=True,
        dead_letter_queue='retry_failed',
        enable_metrics=True
    )
    
    # Счетчик попыток для демонстрации
    attempt_count = {}
    
    @queue.handler
    async def unreliable_processor(data: Dict[str, Any]) -> None:
        """Ненадежный обработчик для демонстрации retry."""
        job_data = data.get('data', {})
        task_id = job_data.get('task_id', 'unknown')
        
        # Увеличиваем счетчик попыток
        attempt_count[task_id] = attempt_count.get(task_id, 0) + 1
        current_attempt = attempt_count[task_id]
        
        logger.info(f"Попытка {current_attempt} для задачи {task_id}")
        
        # Имитируем неудачу для первых двух попыток
        if current_attempt < 3:
            await asyncio.sleep(0.5)
            raise ValueError(f"Временная ошибка для задачи {task_id}")
        
        # Успешная обработка на третьей попытке
        await asyncio.sleep(0.2)
        logger.info(f"Задача {task_id} успешно обработана на попытке {current_attempt}")
    
    # Добавляем задачи
    for i in range(5):
        await queue.push('App\\Jobs\\UnreliableJob', {
            'task_id': f'task_{i}',
            'data': f'Unreliable task {i}'
        })
    
    # Обрабатываем задачи
    try:
        await asyncio.wait_for(queue.listen(), timeout=20.0)
    except asyncio.TimeoutError:
        pass
    
    # Показываем статистику retry
    retry_stats = queue.get_retry_statistics()
    logger.info(f"Retry статистика:")
    logger.info(f"  Всего retry: {retry_stats['total_retries']}")
    logger.info(f"  Успешные retry: {retry_stats['successful_retries']}")
    logger.info(f"  Неудачные retry: {retry_stats['failed_retries']}")
    logger.info(f"  Задачи в dead letter: {retry_stats['dead_letter_jobs']}")
    logger.info(f"  Процент успеха: {retry_stats['success_rate']:.1f}%")
    
    # Показываем задачи в dead letter queue
    dead_letter_jobs = await queue.get_dead_letter_jobs()
    if dead_letter_jobs:
        logger.info(f"Задач в dead letter queue: {len(dead_letter_jobs)}")
    
    await redis_client.close()


async def graceful_shutdown_example():
    """Пример graceful shutdown."""
    logger.info("=== Пример graceful shutdown ===")
    
    redis_client = await create_redis_client()
    
    queue = AsyncQueue(
        client=redis_client,
        queue='shutdown_worker',
        max_concurrent_jobs=3,
        enable_metrics=True
    )
    
    @queue.handler
    async def long_running_task(data: Dict[str, Any]) -> None:
        """Долго выполняющаяся задача."""
        job_data = data.get('data', {})
        task_id = job_data.get('task_id', 'unknown')
        
        logger.info(f"Начинаем долгую задачу {task_id}")
        
        # Имитируем долгую работу
        for i in range(10):
            await asyncio.sleep(1)
            logger.info(f"Задача {task_id}: шаг {i+1}/10")
        
        logger.info(f"Задача {task_id} завершена")
    
    # Добавляем несколько долгих задач
    for i in range(3):
        await queue.push('App\\Jobs\\LongJob', {
            'task_id': f'long_task_{i}',
            'duration': 10
        })
    
    # Запускаем обработку в фоне
    listen_task = asyncio.create_task(queue.listen())
    
    # Ждем немного, затем инициируем shutdown
    await asyncio.sleep(5)
    logger.info("Инициируем graceful shutdown...")
    queue.shutdown()
    
    # Ждем завершения
    try:
        await asyncio.wait_for(listen_task, timeout=15.0)
    except asyncio.TimeoutError:
        logger.warning("Shutdown timeout")
    
    logger.info("Graceful shutdown завершен")
    
    await redis_client.close()


async def metrics_monitoring_example():
    """Пример мониторинга метрик."""
    logger.info("=== Пример мониторинга метрик ===")
    
    redis_client = await create_redis_client()
    
    queue = AsyncQueue(
        client=redis_client,
        queue='metrics_worker',
        max_concurrent_jobs=5,
        enable_metrics=True,
        metrics_history_size=1000
    )
    
    @queue.handler
    async def monitored_task(data: Dict[str, Any]) -> None:
        """Задача с мониторингом."""
        job_data = data.get('data', {})
        task_type = job_data.get('type', 'default')
        duration = job_data.get('duration', 0.5)
        
        # Имитируем работу с разной продолжительностью
        await asyncio.sleep(duration)
        
        # Иногда генерируем ошибки для демонстрации
        if task_type == 'error' and hash(str(job_data)) % 3 == 0:
            raise RuntimeError(f"Случайная ошибка в задаче {task_type}")
    
    # Добавляем разнообразные задачи
    task_types = ['fast', 'medium', 'slow', 'error']
    for i in range(20):
        task_type = task_types[i % len(task_types)]
        duration = {'fast': 0.1, 'medium': 0.5, 'slow': 1.0, 'error': 0.3}[task_type]
        
        await queue.push('App\\Jobs\\MonitoredJob', {
            'type': task_type,
            'duration': duration,
            'task_id': i
        })
    
    # Обрабатываем задачи
    try:
        await asyncio.wait_for(queue.listen(), timeout=25.0)
    except asyncio.TimeoutError:
        pass
    
    # Показываем детальные метрики
    metrics = queue.get_metrics()
    if metrics:
        logger.info("=== Детальные метрики ===")
        
        general = metrics['general']
        logger.info(f"Общие метрики:")
        logger.info(f"  Всего обработано: {general['total_processed']}")
        logger.info(f"  Успешно: {general['total_successful']}")
        logger.info(f"  Неудачно: {general['total_failed']}")
        logger.info(f"  Процент успеха: {general['success_rate']:.1f}%")
        logger.info(f"  Время работы: {general['uptime_seconds']:.1f} сек")
        
        performance = metrics['performance']
        logger.info(f"Производительность:")
        logger.info(f"  Throughput: {performance['throughput_per_second']:.2f} задач/сек")
        logger.info(f"  Среднее время: {performance['avg_processing_time']:.3f} сек")
        logger.info(f"  Мин время: {performance['min_processing_time']:.3f} сек")
        logger.info(f"  Макс время: {performance['max_processing_time']:.3f} сек")
        
        job_types = metrics['job_types']
        logger.info(f"Метрики по типам задач:")
        for job_name, job_metrics in job_types.items():
            logger.info(f"  {job_name}:")
            logger.info(f"    Всего: {job_metrics['total']}")
            logger.info(f"    Успешно: {job_metrics['successful']}")
            logger.info(f"    Процент успеха: {job_metrics['success_rate']:.1f}%")
            logger.info(f"    Среднее время: {job_metrics['avg_processing_time']:.3f} сек")
        
        errors = metrics['errors']
        if errors['error_counts']:
            logger.info(f"Ошибки:")
            for error_type, count in errors['error_counts'].items():
                logger.info(f"  {error_type}: {count}")
    
    # Показываем последние задачи
    recent_jobs = queue.get_recent_jobs(limit=5)
    logger.info("Последние 5 задач:")
    for job in recent_jobs:
        status = "✅" if job['success'] else "❌"
        logger.info(f"  {status} {job['name']} - {job['processing_time']:.3f}с")
    
    await redis_client.close()


async def main():
    """Главная функция с примерами."""
    logger.info("Запуск примеров асинхронной очереди LaraQueue")
    
    try:
        # Запускаем все примеры
        await basic_async_example()
        await asyncio.sleep(2)
        
        await high_performance_example()
        await asyncio.sleep(2)
        
        await retry_and_error_handling_example()
        await asyncio.sleep(2)
        
        await graceful_shutdown_example()
        await asyncio.sleep(2)
        
        await metrics_monitoring_example()
        
    except Exception as e:
        logger.error(f"Ошибка в примерах: {e}")
    
    logger.info("Все примеры завершены")


if __name__ == "__main__":
    # Запускаем примеры
    asyncio.run(main())
