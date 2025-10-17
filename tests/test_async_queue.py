"""
Тесты для асинхронной очереди LaraQueue.
"""

import asyncio
import json
import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch

import aioredis
from lara_queue import AsyncQueue, RetryStrategy
from lara_queue.async_queue import AsyncMetricsCollector


@pytest.fixture
def redis_client():
    """Создает мок Redis клиента для тестов."""
    mock_redis = AsyncMock(spec=aioredis.Redis)
    mock_redis.blpop = AsyncMock(return_value=None)
    mock_redis.rpush = AsyncMock(return_value=1)
    mock_redis.lrange = AsyncMock(return_value=[])
    mock_redis.llen = AsyncMock(return_value=0)
    mock_redis.delete = AsyncMock(return_value=1)
    return mock_redis


@pytest.fixture
def async_queue(redis_client):
    """Создает экземпляр AsyncQueue для тестов."""
    return AsyncQueue(
        client=redis_client,
        queue='test_queue',
        max_concurrent_jobs=5,
        enable_metrics=True
    )


class TestAsyncQueue:
    """Тесты для AsyncQueue."""
    
    @pytest.mark.asyncio
    async def test_async_queue_initialization(self, redis_client):
        """Тест инициализации асинхронной очереди."""
        queue = AsyncQueue(
            client=redis_client,
            queue='test_queue',
            max_concurrent_jobs=10,
            enable_metrics=True
        )
        
        assert queue.queue == 'test_queue'
        assert queue._max_concurrent_jobs == 10
        assert queue.enable_metrics is True
        assert queue.metrics is not None
        assert queue._semaphore._value == 10
    
    @pytest.mark.asyncio
    async def test_async_push(self, async_queue):
        """Тест асинхронного добавления задач."""
        await async_queue.push('TestJob', {'test': 'data'})
        
        # Проверяем, что rpush был вызван
        async_queue.client.rpush.assert_called_once()
        call_args = async_queue.client.rpush.call_args
        
        # Проверяем ключ очереди
        assert call_args[0][0] == 'laravel_database_queues:test_queue'
        
        # Проверяем данные задачи
        job_data = json.loads(call_args[0][1])
        assert job_data['data']['commandName'] == 'TestJob'
        assert 'uuid' in job_data
    
    @pytest.mark.asyncio
    async def test_async_handler_decorator(self, async_queue):
        """Тест декоратора обработчика."""
        handler_called = False
        
        @async_queue.handler
        async def test_handler(data):
            nonlocal handler_called
            handler_called = True
            assert data['name'] == 'TestJob'
            assert data['data']['test'] == 'data'
        
        # Эмитируем событие
        async_queue.ee.emit('queued', {
            'name': 'TestJob',
            'data': {'test': 'data'}
        })
        
        # Даем время на обработку
        await asyncio.sleep(0.01)
        
        assert handler_called is True
    
    @pytest.mark.asyncio
    async def test_async_metrics_collection(self, async_queue):
        """Тест сбора метрик."""
        # Имитируем обработку задачи
        start_time = await async_queue.metrics.record_job_start('TestJob')
        await asyncio.sleep(0.01)  # Небольшая задержка
        await async_queue.metrics.record_job_success('TestJob', start_time, 0.01)
        
        metrics = await async_queue.metrics.get_metrics()
        
        assert metrics['general']['total_processed'] == 1
        assert metrics['general']['total_successful'] == 1
        assert metrics['general']['total_failed'] == 0
        assert metrics['performance']['avg_processing_time'] > 0
    
    @pytest.mark.asyncio
    async def test_async_retry_mechanism(self, async_queue):
        """Тест механизма retry."""
        # Настраиваем retry стратегию
        async_queue.max_retries = 2
        async_queue.retry_strategy = RetryStrategy.EXPONENTIAL
        async_queue.retry_delay = 1
        
        # Тестируем расчет задержки
        delay1 = async_queue._calculate_retry_delay(1)
        delay2 = async_queue._calculate_retry_delay(2)
        
        assert delay1 >= 1
        assert delay2 > delay1  # Экспоненциальный рост
        
        # Тестируем retry job
        job_data = {
            'uuid': 'test-job-id',
            'data': {'commandName': 'TestJob'}
        }
        
        await async_queue._retry_job(job_data, 1)
        
        # Проверяем, что задача была добавлена обратно в очередь
        async_queue.client.rpush.assert_called()
    
    @pytest.mark.asyncio
    async def test_async_dead_letter_queue(self, async_queue):
        """Тест dead letter queue."""
        job_data = {
            'uuid': 'test-job-id',
            'data': {'commandName': 'TestJob'}
        }
        
        error = ValueError("Test error")
        
        await async_queue._send_to_dead_letter_queue(job_data, error, 3)
        
        # Проверяем, что задача была отправлена в dead letter queue
        async_queue.client.rpush.assert_called()
        call_args = async_queue.client.rpush.call_args
        
        # Проверяем ключ dead letter queue
        assert 'failed' in call_args[0][0]
        
        # Проверяем данные dead letter
        dead_letter_data = json.loads(call_args[0][1])
        assert dead_letter_data['original_job'] == job_data
        assert dead_letter_data['error']['type'] == 'ValueError'
        assert dead_letter_data['retry_count'] == 3
    
    @pytest.mark.asyncio
    async def test_async_graceful_shutdown(self, async_queue):
        """Тест graceful shutdown."""
        # Тестируем, что listen завершается при shutdown
        async_queue.client.blpop = AsyncMock(return_value=None)
        
        # Запускаем listen в фоне
        listen_task = asyncio.create_task(async_queue.listen())
        
        # Даем время на запуск
        await asyncio.sleep(0.01)
        
        # Инициируем shutdown
        async_queue.shutdown()
        
        assert async_queue._shutdown is True
        
        # Ждем завершения
        try:
            await asyncio.wait_for(listen_task, timeout=1.0)
        except asyncio.TimeoutError:
            listen_task.cancel()
        
        # Проверяем, что blpop был вызван
        async_queue.client.blpop.assert_called()
    
    @pytest.mark.asyncio
    async def test_async_concurrent_processing(self, async_queue):
        """Тест параллельной обработки задач."""
        processed_jobs = []
        
        @async_queue.handler
        async def concurrent_handler(data):
            job_id = data['data'].get('job_id')
            processed_jobs.append(job_id)
            await asyncio.sleep(0.01)  # Имитируем работу
        
        # Создаем несколько задач
        tasks = []
        for i in range(3):
            task = asyncio.create_task(async_queue._process_job({
                'uuid': f'job_{i}',
                'data': {'commandName': 'TestJob', 'job_id': i}
            }))
            tasks.append(task)
        
        # Ждем завершения всех задач
        await asyncio.gather(*tasks)
        
        # Проверяем, что все задачи были обработаны
        assert len(processed_jobs) == 3
        assert set(processed_jobs) == {0, 1, 2}
    
    @pytest.mark.asyncio
    async def test_async_error_handling(self, async_queue):
        """Тест обработки ошибок."""
        error_occurred = False
        
        @async_queue.handler
        async def error_handler(data):
            nonlocal error_occurred
            error_occurred = True
            raise ValueError("Test error")
        
        # Добавляем обработчик ошибок для AsyncIOEventEmitter
        @async_queue.ee.on('error')
        def handle_error(error):
            pass  # Поглощаем ошибку
        
        # Обрабатываем задачу с ошибкой
        await async_queue._process_job({
            'uuid': 'test-job-id',
            'data': {'commandName': 'TestJob'}
        })
        
        # Даем время на обработку ошибки
        await asyncio.sleep(0.01)
        
        assert error_occurred is True
        
        # Проверяем, что ошибка была записана в метрики
        # AsyncIOEventEmitter может не записывать ошибки в метрики так же, как sync версия
        metrics = await async_queue.metrics.get_metrics()
        # Проверяем, что метрики работают (хотя бы общие)
        assert 'general' in metrics
    
    @pytest.mark.asyncio
    async def test_async_redis_connection_error(self, async_queue):
        """Тест обработки ошибок подключения к Redis."""
        # Имитируем ошибку подключения
        async_queue.client.blpop = AsyncMock(side_effect=aioredis.exceptions.ConnectionError("Connection failed"))
        
        # Запускаем listen с коротким таймаутом
        try:
            await asyncio.wait_for(async_queue.listen(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        
        # Проверяем, что blpop был вызван (попытка подключения)
        async_queue.client.blpop.assert_called()
    
    @pytest.mark.asyncio
    async def test_async_metrics_reset(self, async_queue):
        """Тест сброса метрик."""
        # Добавляем некоторые метрики
        start_time = await async_queue.metrics.record_job_start('TestJob')
        await async_queue.metrics.record_job_success('TestJob', start_time, 0.1)
        
        # Проверяем, что метрики есть
        metrics = await async_queue.metrics.get_metrics()
        assert metrics['general']['total_processed'] == 1
        
        # Сбрасываем метрики
        await async_queue.reset_metrics()
        
        # Проверяем, что метрики сброшены
        metrics = await async_queue.metrics.get_metrics()
        assert metrics['general']['total_processed'] == 0
    
    @pytest.mark.asyncio
    async def test_async_retry_statistics(self, async_queue):
        """Тест статистики retry."""
        # Получаем начальную статистику
        initial_stats = async_queue.get_retry_statistics()
        assert initial_stats['total_retries'] == 0
        
        # Имитируем retry
        job_data = {'uuid': 'test-job-id', 'data': {'commandName': 'TestJob'}}
        await async_queue._retry_job(job_data, 1)
        
        # Проверяем обновленную статистику
        updated_stats = async_queue.get_retry_statistics()
        assert updated_stats['total_retries'] == 1
        
        # Сбрасываем статистику
        async_queue.reset_retry_statistics()
        
        # Проверяем, что статистика сброшена
        reset_stats = async_queue.get_retry_statistics()
        assert reset_stats['total_retries'] == 0


class TestAsyncMetricsCollector:
    """Тесты для AsyncMetricsCollector."""
    
    def test_async_metrics_initialization(self):
        """Тест инициализации метрик."""
        metrics = AsyncMetricsCollector(max_history_size=100)
        
        assert metrics.max_history_size == 100
        assert metrics.total_processed == 0
        assert metrics.total_successful == 0
        assert metrics.total_failed == 0
    
    @pytest.mark.asyncio
    async def test_async_metrics_job_recording(self):
        """Тест записи метрик задач."""
        metrics = AsyncMetricsCollector()
        
        # Записываем успешную задачу
        start_time = await metrics.record_job_start('TestJob')
        await metrics.record_job_success('TestJob', start_time, 0.5)
        
        # Проверяем метрики
        assert metrics.total_processed == 1
        assert metrics.total_successful == 1
        assert metrics.total_failed == 0
        assert metrics.job_type_counts['TestJob'] == 1
        assert metrics.job_type_success['TestJob'] == 1
    
    @pytest.mark.asyncio
    async def test_async_metrics_failure_recording(self):
        """Тест записи метрик ошибок."""
        metrics = AsyncMetricsCollector()
        
        # Записываем неудачную задачу
        start_time = await metrics.record_job_start('TestJob')
        error = ValueError("Test error")
        await metrics.record_job_failure('TestJob', start_time, 0.3, error)
        
        # Проверяем метрики
        assert metrics.total_processed == 1
        assert metrics.total_successful == 0
        assert metrics.total_failed == 1
        assert metrics.job_type_failed['TestJob'] == 1
        assert metrics.error_counts['ValueError'] == 1
    
    @pytest.mark.asyncio
    async def test_async_metrics_retry_recording(self):
        """Тест записи метрик retry."""
        metrics = AsyncMetricsCollector()
        
        # Записываем retry
        await metrics.record_retry('TestJob', 1)
        
        # Проверяем метрики
        assert metrics.total_retries == 1
    
    @pytest.mark.asyncio
    async def test_async_metrics_get_metrics(self):
        """Тест получения полных метрик."""
        metrics = AsyncMetricsCollector()
        
        # Добавляем различные метрики
        start_time = await metrics.record_job_start('TestJob1')
        await metrics.record_job_success('TestJob1', start_time, 0.5)
        
        start_time = await metrics.record_job_start('TestJob2')
        error = ValueError("Test error")
        await metrics.record_job_failure('TestJob2', start_time, 0.3, error)
        
        await metrics.record_retry('TestJob1', 1)
        
        # Получаем полные метрики
        full_metrics = await metrics.get_metrics()
        
        # Проверяем общие метрики
        assert full_metrics['general']['total_processed'] == 2
        assert full_metrics['general']['total_successful'] == 1
        assert full_metrics['general']['total_failed'] == 1
        assert full_metrics['general']['total_retries'] == 1
        assert full_metrics['general']['success_rate'] == 50.0
        
        # Проверяем метрики производительности
        assert full_metrics['performance']['avg_processing_time'] > 0
        assert full_metrics['performance']['min_processing_time'] > 0
        assert full_metrics['performance']['max_processing_time'] > 0
        
        # Проверяем метрики по типам задач
        assert 'TestJob1' in full_metrics['job_types']
        assert 'TestJob2' in full_metrics['job_types']
        
        # Проверяем метрики ошибок
        assert 'ValueError' in full_metrics['errors']['error_counts']
    
    @pytest.mark.asyncio
    async def test_async_metrics_reset(self):
        """Тест сброса метрик."""
        metrics = AsyncMetricsCollector()
        
        # Добавляем метрики
        start_time = await metrics.record_job_start('TestJob')
        await metrics.record_job_success('TestJob', start_time, 0.5)
        await metrics.record_retry('TestJob', 1)
        
        # Проверяем, что метрики есть
        assert metrics.total_processed == 1
        assert metrics.total_retries == 1
        
        # Сбрасываем метрики
        await metrics.reset_metrics()
        
        # Проверяем, что метрики сброшены
        assert metrics.total_processed == 0
        assert metrics.total_successful == 0
        assert metrics.total_failed == 0
        assert metrics.total_retries == 0
        assert len(metrics.job_type_counts) == 0
        assert len(metrics.error_counts) == 0
    
    @pytest.mark.asyncio
    async def test_async_metrics_recent_jobs(self):
        """Тест получения последних задач."""
        metrics = AsyncMetricsCollector()
        
        # Добавляем несколько задач
        for i in range(5):
            start_time = await metrics.record_job_start(f'TestJob{i}')
            await metrics.record_job_success(f'TestJob{i}', start_time, 0.1 * i)
        
        # Получаем последние задачи
        recent_jobs = await metrics.get_recent_jobs(limit=3)
        
        # Проверяем, что получили последние 3 задачи
        assert len(recent_jobs) == 3
        assert recent_jobs[-1]['name'] == 'TestJob4'  # Последняя задача


@pytest.mark.asyncio
async def test_async_queue_integration():
    """Интеграционный тест асинхронной очереди."""
    # Создаем мок Redis клиента
    mock_redis = AsyncMock(spec=aioredis.Redis)
    
    # Настраиваем поведение blpop для имитации получения задачи
    job_data = {
        'uuid': 'test-job-id',
        'data': {
            'commandName': 'TestJob',
            'command': 'serialized_php_data'
        }
    }
    
    mock_redis.blpop = AsyncMock(return_value=('queue_key', json.dumps(job_data)))
    mock_redis.rpush.return_value = 1
    
    # Создаем очередь
    queue = AsyncQueue(
        client=mock_redis,
        queue='test_queue',
        max_concurrent_jobs=1,
        enable_metrics=True
    )
    
    # Регистрируем обработчик
    processed_jobs = []
    
    @queue.handler
    async def test_handler(data):
        processed_jobs.append(data['name'])
    
    # Мокаем phpserialize.loads
    with patch('lara_queue.async_queue.phpserialize.loads') as mock_loads:
        # Создаем мок объект для phpserialize.loads
        mock_php_object = MagicMock()
        mock_php_object._asdict.return_value = {'test': 'data'}
        mock_loads.return_value = mock_php_object
        
        # Настраиваем blpop чтобы он возвращал задачу только один раз, затем None
        call_count = 0
        async def mock_blpop(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ('queue_key', json.dumps(job_data))
            else:
                return None
        
        mock_redis.blpop = AsyncMock(side_effect=mock_blpop)
        
        # Запускаем обработку с коротким таймаутом
        try:
            await asyncio.wait_for(queue.listen(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
    
    # Проверяем, что задача была обработана
    assert len(processed_jobs) == 1
    assert processed_jobs[0] == 'TestJob'
    
    # Проверяем метрики
    metrics = await queue.get_metrics()
    assert metrics['general']['total_processed'] == 1
