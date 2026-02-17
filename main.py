import concurrent.futures
import requests
from datetime import datetime, timezone
import queue
import csv
import numpy as np
import matplotlib.pyplot as plt

# Эта программа скачивает данные об акциях с сайта Yahoo Finance
# и рисует график цен

# Настройки для запроса к сайту
user_agent_key = "User-Agent"
user_agent_value = "Mozilla/5.0"
headers = {user_agent_key: user_agent_value}


def get_ticker(file: str):
    """
    Читает файл со списком тикеров (названий акций)
    Возвращает по одному тикеру за раз через yield
    """
    with open(file) as file:
        for line in file:
            ticker = line.strip()
            yield ticker


def get_history_data(ticker: str, start_date: str, end_date: str, interval: str = "1wk"):
    """
    Скачивает данные с Yahoo Finance для одного тикера

    ticker - название акции (например 'AAPL')
    start_date - дата начала в формате 'дд.мм.гг'
    end_date - дата конца в формате 'дд.мм.гг'
    interval - период (1wk = одна неделя)

    Возвращает словарь с тикером и данными от Yahoo
    """
    # Переводим даты в формат timestamp (число секунд с 1970 года)
    per2 = int(datetime.strptime(end_date, '%d.%m.%y').replace(tzinfo=timezone.utc).timestamp())
    per1 = int(datetime.strptime(start_date, '%d.%m.%y').replace(tzinfo=timezone.utc).timestamp())

    params = {"period1": str(per1), "period2": str(per2),
              "interval": interval, "includeAdjustedClose": "true"}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"

    print("Отправляем запрос на сайт")
    response = requests.get(url, headers=headers, params=params)
    print("Запрос получен")

    return {"ticker": ticker, "data": response.json()}


def process_results(result_data):
    """
    Сохраняет данные в CSV файл

    result_data - словарь с тикером и данными от Yahoo

    Возвращает имя созданного файла
    """
    ticker = result_data["ticker"]
    yahoo_data = result_data["data"]
    parsed_data = parse_yahoo_data(ticker, yahoo_data)

    if not parsed_data:
        print(f"Нет данных для тикера {ticker}")
        return None

    filename = f"{ticker}.csv"

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            'ticker', 'date', 'high', 'low'
        ])

        writer.writeheader()
        writer.writerows(parsed_data)

    print(f"Создан файл: {filename}")
    return filename


def parse_yahoo_data(ticker, yahoo_data):
    """
    Достает нужные данные из того, что вернул Yahoo Finance

    ticker - название акции
    yahoo_data - сырые данные от Yahoo

    Возвращает список словарей с датой, максимальной и минимальной ценой
    """
    try:
        result = yahoo_data["chart"]["result"][0]
        timestamps = result["timestamp"]  # Время в формате timestamp
        quote = result["indicators"]["quote"][0]
        highs = quote["high"]  # Максимальные цены за неделю
        lows = quote["low"]  # Минимальные цены за неделю
        parsed_data = []

        for i in range(len(timestamps)):
            # Переводим timestamp в нормальную дату
            date = datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d')
            parsed_data.append({
                'ticker': ticker,
                'date': date,
                'high': highs[i] if highs[i] else '',  # Если цены нет, ставим пустую строку
                'low': lows[i] if lows[i] else ''
            })
        return parsed_data
    except Exception as e:
        print(f"Ошибка парсинга данных для {ticker}: {e}")
        return


def read_file(filename):
    """
    Читает CSV файл и возвращает массив цен (колонка high)

    filename - имя файла

    Возвращает numpy массив с ценами
    """
    prices = []
    try:
        with open(filename) as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['high']:  # Проверяем, что цена не пустая
                    prices.append(float(row['high']))
    except FileNotFoundError:
        print(f"Файл {filename} не найден")
    except Exception as e:
        print(f"Ошибка чтения файла {filename}: {e}")
    return np.array(prices)


start_time = "01.01.20"  # Начальная дата
end_time = "02.11.25"  # Конечная дата
data_queue = queue.Queue()  # Очередь для передачи данных между потоками
futures = []
comp_names = ['AAPL', 'MSFT', 'AMZN', 'NVDA', 'TSLA', 'GOOGL', 'META', 'BRK-B', 'UNH', 'JPM']

# Скачиваем данные с Yahoo Finance
print("Начинаем скачивание данных...")
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Запускаем скачивание для всех тикеров из файла
    for ticker in get_ticker("ticker_names.txt"):
        future = executor.submit(get_history_data, ticker, start_time, end_time, "1wk")
        futures.append(future)

    # Собираем результаты по мере их готовности
    for future in concurrent.futures.as_completed(futures):
        try:
            result = future.result()
            data_queue.put(result)
        except Exception as e:
            print(f"Ошибка: {e}")

# Сохраняем данные в CSV файлы
print("Сохраняем данные в файлы...")
with concurrent.futures.ThreadPoolExecutor() as result_executor:
    result_futures = []

    while not data_queue.empty():
        result = data_queue.get()

        if "error" in result:
            print(f"Пропускаем {result['ticker']} из-за ошибки: {result['error']}")
            continue

        future = result_executor.submit(process_results, result)
        result_futures.append(future)

    for future in concurrent.futures.as_completed(result_futures):
        try:
            filename = future.result()
        except Exception as e:
            print(f"Ошибка сохранения: {e}")

# Читаем данные из файлов
print("Читаем данные из файлов...")
with concurrent.futures.ThreadPoolExecutor() as read_file_executor:
    future_to_name = {
        read_file_executor.submit(read_file, f"{name}.csv"): name
        for name in comp_names
    }
    all_data = {}
    for future in concurrent.futures.as_completed(future_to_name):
        name = future_to_name[future]
        try:
            data = future.result()
            if len(data) > 0:
                all_data[name] = data
            else:
                print(f"Нет данных для {name}")
        except Exception as e:
            print(f"Ошибка при чтении {name}: {e}")

# Рисуем график
print("Рисуем график...")
plt.figure(figsize=(12, 8))
for name, prices in all_data.items():
    if len(prices) > 0:
        x = range(len(prices))
        plt.plot(x, prices, label=name, linewidth=2)

plt.xlabel('Недели с начала 2020 года')
plt.ylabel('Цена High ($)')
plt.title('Динамика цен акций (недельные данные)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

print("Готово!")
