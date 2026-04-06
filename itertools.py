import itertools
import operator
import time

def generate_mock_logs(num_records):
    """Генерация тестовых логов запросов с использованием itertools."""
    endpoints = ['/api/v1/auth', '/api/v1/data', '/api/v1/users', '/health']
    methods = ['GET', 'POST', 'PUT']
    status_codes = [200, 201, 400, 401, 403, 500, 502]
    
    # 1. product: Создание декартова произведения маршрутов и методов
    all_routes = list(itertools.product(methods, endpoints))
    
    # 2. cycle: Балансировщик нагрузки (Round-Robin) по 3 серверам
    servers = itertools.cycle(['srv-alpha', 'srv-beta', 'srv-gamma'])
    
    # 3. count: Генерация уникальных идентификаторов транзакций (начиная с 1000)
    tx_ids = itertools.count(start=1000)
    
    logs = []
    # 4. islice: Ограничение бесконечных генераторов до нужного количества записей
    for _ in itertools.islice(itertools.count(), num_records):
        route = all_routes[hash(str(_)) % len(all_routes)]
        logs.append({
            'tx_id': next(tx_ids),
            'server': next(servers),
            'method': route[0],
            'endpoint': route[1],
            'status': status_codes[hash(str(_ * 3)) % len(status_codes)],
            'bytes_sent': (hash(str(_)) % 5000) + 100,
            'bytes_received': (hash(str(_ * 2)) % 2000) + 50,
            'response_time_ms': (hash(str(_ * 5)) % 300) + 10
        })
    return logs

def process_logs(logs):
    """Многопоточная обработка логов через итераторы."""
    
    # 5. tee: Разветвление потока логов на 3 независимых итератора для разного анализа
    iter_errors, iter_stats, iter_bandwidth = itertools.tee(logs, 3)

    print("--- АНАЛИЗ ОШИБОК ---")
    # 6. filterfalse: Отсеивание успешных запросов (оставляем только ошибки >= 400)
    errors = itertools.filterfalse(lambda x: x['status'] < 400, iter_errors)
    # 7. islice: Берем только первые 5 ошибок для алерта
    top_errors = list(itertools.islice(errors, 5))
    for err in top_errors:
        print(f"Alert: Server {err['server']} returned {err['status']} on {err['endpoint']}")

    print("\n--- СТАТИСТИКА ПО МАРШРУТАМ ---")
    # 8. groupby: Группировка данных по эндпоинтам. Требует предварительной сортировки.
    sorted_for_grouping = sorted(iter_stats, key=lambda x: x['endpoint'])
    for endpoint, group in itertools.groupby(sorted_for_grouping, key=lambda x: x['endpoint']):
        group_list = list(group)
        print(f"Endpoint {endpoint}: {len(group_list)} запросов")

    print("\n--- АНАЛИЗ ТРАФИКА И НАГРУЗКИ ---")
    # 9. starmap: Вычисление общего объема данных (отправлено + получено) для каждого запроса
    traffic_pairs = ((log['bytes_sent'], log['bytes_received']) for log in iter_bandwidth)
    total_bytes_per_req = list(itertools.starmap(operator.add, traffic_pairs))
    
    # 10. accumulate: Нарастающий итог переданного трафика в системе
    running_total_traffic = list(itertools.accumulate(total_bytes_per_req))
    print(f"Суммарный обработанный трафик: {running_total_traffic[-1]} байт")

def analyze_server_stability(logs):
    """Анализ стабильности с использованием фильтраций по условию."""
    print("\n--- АНАЛИЗ СТАБИЛЬНОСТИ ---")
    sorted_by_time = sorted(logs, key=lambda x: x['response_time_ms'])
    
    # 11. dropwhile: Отбрасываем все быстрые запросы, пока не встретим первый медленный (>250мс)
    slow_requests_start = itertools.dropwhile(lambda x: x['response_time_ms'] <= 250, sorted_by_time)
    
    # 12. takewhile: Берем медленные запросы, но останавливаемся на критических (>290мс)
    moderate_slow = list(itertools.takewhile(lambda x: x['response_time_ms'] <= 290, slow_requests_start))
    print(f"Найдено {len(moderate_slow)} запросов с пограничным временем ответа (251-290 мс).")

def main():
    # 13. chain: Объединение нескольких источников логов (например, логи за разные часы)
    logs_hour_1 = generate_mock_logs(50)
    logs_hour_2 = generate_mock_logs(50)
    
    all_logs = list(itertools.chain(logs_hour_1, logs_hour_2))
    
    print(f"Всего загружено логов: {len(all_logs)}\n")
    process_logs(all_logs)
    analyze_server_stability(all_logs)

if __name__ == "__main__":
    main()
