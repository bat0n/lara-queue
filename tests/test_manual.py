"""Manual test script for interactive testing with Redis and Laravel.

This script is not run by pytest. Use it for manual testing:
    python -m tests.test_manual

Requirements:
- Redis server running on localhost:6379
- Optional: Laravel application with queue worker
"""
import sys
import time
from redis import Redis
from lara_queue import Queue


def test_push_to_laravel():
    """Test 1: Push a job to Laravel queue."""
    print("\n" + "="*60)
    print("TEST 1: Отправка задачи в Laravel очередь")
    print("="*60)
    
    try:
        r = Redis(host='localhost', port=6379, db=0, decode_responses=False)
        r.ping()
        print("✅ Подключено к Redis")
    except Exception as e:
        print(f"❌ Не удалось подключиться к Redis: {e}")
        print("Убедитесь, что Redis запущен: redis-server")
        return False
    
    queue = Queue(r, queue='laravel')
    
    job_data = {
        'a': 'Hello',
        'b': 'from',
        'c': 'Python!'
    }
    
    try:
        queue.push('App\\Jobs\\TestJob', job_data)
        print(f"✅ Задача отправлена в очередь 'laravel'")
        print(f"   Данные: {job_data}")
        print("\nТеперь запустите Laravel worker:")
        print("   php artisan queue:work --queue=laravel")
        return True
    except Exception as e:
        print(f"❌ Ошибка при отправке: {e}")
        return False


def test_listen_for_jobs():
    """Test 2: Listen for jobs from Laravel."""
    print("\n" + "="*60)
    print("TEST 2: Прослушивание Python очереди")
    print("="*60)
    
    try:
        r = Redis(host='localhost', port=6379, db=0, decode_responses=False)
        r.ping()
        print("✅ Подключено к Redis")
    except Exception as e:
        print(f"❌ Не удалось подключиться к Redis: {e}")
        return False
    
    queue = Queue(r, queue='python')
    
    print("\n📋 Ожидание задач от Laravel...")
    print("Отправьте задачу из Laravel:")
    print("   dispatch(new TestJob('a', 'b', 'c'))->onQueue('python');")
    print("\nНажмите Ctrl+C для остановки\n")
    
    jobs_received = [0]  # Use list to modify in nested function
    
    @queue.handler
    def handle(data):
        jobs_received[0] += 1
        print(f"\n{'='*60}")
        print(f"📦 Получена задача #{jobs_received[0]}")
        print(f"{'='*60}")
        print(f"Имя задачи: {data['name']}")
        print(f"Данные: {data['data']}")
        print(f"{'='*60}\n")
    
    try:
        queue.listen()
    except KeyboardInterrupt:
        print(f"\n\n⛔ Остановлено пользователем")
        print(f"📊 Всего получено задач: {jobs_received[0]}")
        return True
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_check_redis_queue():
    """Test 3: Check what's in the Redis queue."""
    print("\n" + "="*60)
    print("TEST 3: Проверка содержимого Redis очереди")
    print("="*60)
    
    try:
        r = Redis(host='localhost', port=6379, db=0, decode_responses=False)
        r.ping()
        print("✅ Подключено к Redis")
    except Exception as e:
        print(f"❌ Не удалось подключиться к Redis: {e}")
        return False
    
    queues_to_check = ['laravel', 'python', 'default']
    
    for queue_name in queues_to_check:
        key = f'laravel_database_queues:{queue_name}'
        length = r.llen(key)
        print(f"\nОчередь '{queue_name}': {length} задач")
        
        if length > 0:
            # Show first job without removing it
            job = r.lindex(key, 0)
            if job:
                import json
                try:
                    job_data = json.loads(job)
                    print(f"  Первая задача: {job_data.get('data', {}).get('commandName', 'N/A')}")
                except:
                    print(f"  Первая задача: {job[:100]}...")
    
    return True


def main():
    """Main menu for manual testing."""
    print("\n" + "="*60)
    print("🧪 LaraQueue - Ручное тестирование")
    print("="*60)
    
    while True:
        print("\nВыберите тест:")
        print("  1. Отправить задачу в Laravel")
        print("  2. Прослушивать очередь Python")
        print("  3. Проверить содержимое очередей Redis")
        print("  4. Выполнить тест 1, затем тест 2")
        print("  0. Выход")
        
        choice = input("\nВаш выбор: ").strip()
        
        if choice == '1':
            test_push_to_laravel()
        elif choice == '2':
            test_listen_for_jobs()
        elif choice == '3':
            test_check_redis_queue()
        elif choice == '4':
            if test_push_to_laravel():
                time.sleep(1)
                test_listen_for_jobs()
        elif choice == '0':
            print("\n👋 До свидания!")
            break
        else:
            print("❌ Неверный выбор")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⛔ Программа прервана")
        sys.exit(0)

