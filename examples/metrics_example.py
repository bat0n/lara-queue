#!/usr/bin/env python3
"""
Example of using metrics system in LaraQueue.

This example demonstrates collection and analysis of queue performance metrics.
"""

import logging
import time
import random
import json
from redis import Redis
from lara_queue import Queue, RetryStrategy

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def simulate_work(duration_range=(0.1, 2.0)):
    """Simulates work with random duration."""
    duration = random.uniform(*duration_range)
    time.sleep(duration)
    return duration

def main():
    """Main function with metrics examples."""
    
    # Connect to Redis
    redis_client = Redis(host='localhost', port=6379, db=0)
    
    print("📊 Examples of using metrics system in LaraQueue")
    print("=" * 60)
    
    # Create queue with metrics enabled
    queue = Queue(
        client=redis_client,
        queue='metrics_demo',
        max_retries=2,
        retry_strategy=RetryStrategy.EXPONENTIAL,
        enable_metrics=True,  # Enable metrics
        metrics_history_size=500  # History size
    )
    
    # Handlers for different job types
    @queue.handler
    def process_email(data):
        """Email job processing."""
        logger.info(f"📧 Обработка email: {data}")
        
        # Имитируем работу
        duration = simulate_work((0.2, 1.5))
        
        # 10% вероятность ошибки
        if random.random() < 0.1:
            raise ValueError("Email service temporarily unavailable")
        
        logger.info(f"✅ Email отправлен за {duration:.2f}s")
    
    @queue.handler
    def process_notification(data):
        """Обработка уведомлений."""
        logger.info(f"🔔 Обработка уведомления: {data}")
        
        # Имитируем работу
        duration = simulate_work((0.1, 0.8))
        
        # 5% вероятность ошибки
        if random.random() < 0.05:
            raise ConnectionError("Notification service connection failed")
        
        logger.info(f"✅ Уведомление отправлено за {duration:.2f}s")
    
    @queue.handler
    def process_report(data):
        """Обработка отчетов."""
        logger.info(f"📈 Генерация отчета: {data}")
        
        # Имитируем долгую работу
        duration = simulate_work((1.0, 3.0))
        
        # 15% вероятность ошибки
        if random.random() < 0.15:
            raise RuntimeError("Report generation failed")
        
        logger.info(f"✅ Отчет сгенерирован за {duration:.2f}s")
    
    @queue.handler
    def process_analytics(data):
        """Обработка аналитики."""
        logger.info(f"📊 Обработка аналитики: {data}")
        
        # Имитируем работу
        duration = simulate_work((0.5, 2.5))
        
        # 8% вероятность ошибки
        if random.random() < 0.08:
            raise Exception("Analytics processing error")
        
        logger.info(f"✅ Аналитика обработана за {duration:.2f}s")
    
    # Функция для отправки тестовых задач
    def send_test_jobs():
        """Отправляет тестовые задачи в очередь."""
        job_types = [
            ('App\\Jobs\\EmailJob', {'recipient': 'user@example.com', 'subject': 'Test Email'}),
            ('App\\Jobs\\NotificationJob', {'user_id': 123, 'message': 'Test Notification'}),
            ('App\\Jobs\\ReportJob', {'type': 'daily', 'date': '2024-01-01'}),
            ('App\\Jobs\\AnalyticsJob', {'metric': 'page_views', 'period': 'hourly'})
        ]
        
        for job_class, job_data in job_types:
            queue.push(job_class, job_data)
            logger.info(f"📤 Отправлена задача: {job_class}")
    
    # Функция для отображения метрик
    def display_metrics():
        """Отображает текущие метрики."""
        metrics = queue.get_metrics()
        if not metrics:
            print("❌ Метрики отключены")
            return
        
        print("\n📊 ТЕКУЩИЕ МЕТРИКИ")
        print("-" * 40)
        
        # Общие метрики
        general = metrics['general']
        print(f"📈 Общие показатели:")
        print(f"  • Всего обработано: {general['total_processed']}")
        print(f"  • Успешных: {general['total_successful']}")
        print(f"  • Неудачных: {general['total_failed']}")
        print(f"  • Retry: {general['total_retries']}")
        print(f"  • Процент успеха: {general['success_rate']:.1f}%")
        print(f"  • Время работы: {general['uptime_seconds']:.1f}s")
        
        # Метрики производительности
        performance = metrics['performance']
        print(f"\n⚡ Производительность:")
        print(f"  • Среднее время обработки: {performance['avg_processing_time']:.3f}s")
        print(f"  • Минимальное время: {performance['min_processing_time']:.3f}s")
        print(f"  • Максимальное время: {performance['max_processing_time']:.3f}s")
        print(f"  • Throughput: {performance['throughput_per_second']:.2f} задач/сек")
        print(f"  • Размер истории: {performance['history_size']}")
        
        # Метрики по типам задач
        job_types = metrics['job_types']
        if job_types:
            print(f"\n🎯 Метрики по типам задач:")
            for job_name, job_metrics in job_types.items():
                print(f"  • {job_name}:")
                print(f"    - Всего: {job_metrics['total']}")
                print(f"    - Успешных: {job_metrics['successful']}")
                print(f"    - Неудачных: {job_metrics['failed']}")
                print(f"    - Процент успеха: {job_metrics['success_rate']:.1f}%")
                print(f"    - Среднее время: {job_metrics['avg_processing_time']:.3f}s")
        
        # Метрики ошибок
        errors = metrics['errors']
        if errors['error_counts']:
            print(f"\n❌ Ошибки:")
            for error_type, count in errors['error_counts'].items():
                print(f"  • {error_type}: {count}")
    
    # Функция для отображения последних задач
    def display_recent_jobs(limit=10):
        """Отображает последние обработанные задачи."""
        recent_jobs = queue.get_recent_jobs(limit)
        if not recent_jobs:
            print("📝 Нет истории задач")
            return
        
        print(f"\n📝 ПОСЛЕДНИЕ {len(recent_jobs)} ЗАДАЧ")
        print("-" * 40)
        
        for job in recent_jobs:
            status = "✅" if job['success'] else "❌"
            timestamp = time.strftime('%H:%M:%S', time.localtime(job['timestamp']))
            print(f"{status} {timestamp} | {job['name']} | {job['processing_time']:.3f}s")
            if not job['success'] and 'error' in job:
                print(f"    Ошибка: {job['error']}")
    
    # Функция для отображения метрик конкретного типа задач
    def display_job_type_metrics(job_name):
        """Отображает метрики для конкретного типа задач."""
        metrics = queue.get_job_type_metrics(job_name)
        if not metrics:
            print(f"❌ Нет метрик для {job_name}")
            return
        
        print(f"\n🎯 МЕТРИКИ ДЛЯ {job_name}")
        print("-" * 40)
        print(f"Всего задач: {metrics['total']}")
        print(f"Успешных: {metrics['successful']}")
        print(f"Неудачных: {metrics['failed']}")
        print(f"Процент успеха: {metrics['success_rate']:.1f}%")
        print(f"Среднее время: {metrics['avg_processing_time']:.3f}s")
        print(f"Минимальное время: {metrics['min_processing_time']:.3f}s")
        print(f"Максимальное время: {metrics['max_processing_time']:.3f}s")
    
    # Функция для отображения сводки производительности
    def display_performance_summary():
        """Отображает сводку производительности."""
        summary = queue.get_performance_summary()
        if not summary:
            print("❌ Нет данных о производительности")
            return
        
        print(f"\n⚡ СВОДКА ПРОИЗВОДИТЕЛЬНОСТИ")
        print("-" * 40)
        
        general = summary['general']
        performance = summary['performance']
        
        print(f"📊 Общая статистика:")
        print(f"  • Обработано задач: {general['total_processed']}")
        print(f"  • Процент успеха: {general['success_rate']:.1f}%")
        print(f"  • Время работы: {general['uptime_seconds']:.1f}s")
        
        print(f"\n🚀 Производительность:")
        print(f"  • Throughput: {performance['throughput_per_second']:.2f} задач/сек")
        print(f"  • Среднее время: {performance['avg_processing_time']:.3f}s")
        print(f"  • Диапазон: {performance['min_processing_time']:.3f}s - {performance['max_processing_time']:.3f}s")
    
    # Демонстрация работы
    print("\n🚀 Запуск демонстрации метрик...")
    
    # Отправляем тестовые задачи
    print("\n📤 Отправка тестовых задач...")
    for i in range(20):
        send_test_jobs()
        time.sleep(0.1)  # Небольшая задержка между отправками
    
    print(f"\n⏳ Ожидание обработки задач...")
    time.sleep(5)  # Ждем обработки
    
    # Отображаем метрики
    display_metrics()
    display_recent_jobs(15)
    display_performance_summary()
    
    # Показываем метрики для конкретного типа задач
    display_job_type_metrics('App\\Jobs\\EmailJob')
    
    # Демонстрация сброса метрик
    print(f"\n🔄 Сброс метрик...")
    queue.reset_metrics()
    
    print(f"\n📊 Метрики после сброса:")
    display_metrics()
    
    # Демонстрация отключения метрик
    print(f"\n🔧 Создание очереди без метрик...")
    queue_no_metrics = Queue(
        client=redis_client,
        queue='no_metrics_demo',
        enable_metrics=False  # Отключаем метрики
    )
    
    @queue_no_metrics.handler
    def simple_handler(data):
        logger.info(f"Простая обработка: {data}")
    
    # Отправляем задачу
    queue_no_metrics.push('SimpleJob', {'data': 'test'})
    
    # Проверяем метрики
    metrics = queue_no_metrics.get_metrics()
    print(f"Метрики в очереди без метрик: {metrics}")
    
    print(f"\n✅ Демонстрация завершена!")
    print(f"\n💡 Рекомендации по использованию метрик:")
    print(f"• Включайте метрики для production мониторинга")
    print(f"• Настройте размер истории под ваши нужды")
    print(f"• Регулярно сбрасывайте метрики для точности")
    print(f"• Используйте метрики для оптимизации производительности")
    print(f"• Анализируйте ошибки для улучшения надежности")

if __name__ == "__main__":
    main()
