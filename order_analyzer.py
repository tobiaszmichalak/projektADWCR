import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import pytz

# Konfiguracja
KAFKA_SERVER = "broker:9092"
ORDERS_TOPIC = "food_orders"
ANALYSIS_WINDOW = 15  # Analiza w oknie 15 minut

# MongoDB konfiguracja
MONGO_HOST = "mongo"
MONGO_PORT = 27017
MONGO_DB = "delivery_service"
MONGO_COLLECTION = "orders"

def create_consumer():
    """Utworzenie konsumenta Kafka dla zamówień"""
    return KafkaConsumer(
        ORDERS_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='order_analyzer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def create_mongo_connection():
    """Utworzenie połączenia z MongoDB"""
    try:
        # Połączenie z uwierzytelnieniem
        client = MongoClient(
            f"mongodb://root:admin@{MONGO_HOST}:{MONGO_PORT}/",
            serverSelectionTimeoutMS=5000
        )
        # Sprawdzenie połączenia
        client.admin.command('ping')
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Próba utworzenia indeksów dla lepszej wydajności
        try:
            collection.create_index([("timestamp", 1)])
            collection.create_index([("city", 1)])
            collection.create_index([("order_id", 1)], unique=True)
            print("Utworzono indeksy MongoDB")
        except Exception as e:
            print(f"Uwaga: Nie można utworzyć indeksów: {e}")
            print("Kontynuowanie bez indeksów...")
        
        print("Połączono z MongoDB")
        return client, collection
    except ConnectionFailure as e:
        print(f"Nie można połączyć się z MongoDB: {e}")
        return None, None
        
def save_to_mongodb(collection, order):
    """Zapisanie zamówienia do MongoDB"""
    try:
        # Konwersja timestamp na datetime dla lepszego indeksowania
        order_copy = order.copy()
        order_copy["timestamp_dt"] = datetime.fromisoformat(order["timestamp"])
        
        # Upsert - zaktualizuj jeśli istnieje, wstaw jeśli nie
        collection.update_one(
            {"order_id": order["order_id"]},
            {"$set": order_copy},
            upsert=True
        )
    except OperationFailure as e:
        print(f"Błąd zapisu do MongoDB: {e}")


def calculate_courier_needs(orders, window_minutes=15):
    """
    Obliczanie optymalnej liczby kurierów na podstawie zamówień
    
    Założenia:
    - Jeden kurier może obsłużyć średnio 2-3 zamówienia na godzinę
    - Na obszarach o dużej gęstości zamówień efektywność wzrasta
    - Złe warunki pogodowe zmniejszają efektywność kurierów
    
    Zwraca: Sugerowaną liczbę kurierów dla danego miasta
    """
    if not orders:
        return 0
    
    # Liczba zamówień w oknie czasowym (15 minut)
    num_orders = len(orders)
    
    # Sprawdzenie warunków pogodowych (z ostatniego zamówienia)
    last_order = orders[-1]
    weather = last_order.get("weather_data", {})
    
    # Podstawowe zapotrzebowanie: 1 kurier obsługuje ~2.5 zamówienia/godz
    # czyli ~0.625 zamówienia na 15 minut
    base_couriers = num_orders / 0.625
    
    # Modyfikatory ze względu na pogodę
    weather_factor = 1.0
    
    # Uwzględnienie opadów
    precipitation = weather.get("precipitation", 0)
    if precipitation > 5:  # Poważne opady
        weather_factor += 0.3
    elif precipitation > 1:  # Umiarkowane opady
        weather_factor += 0.15
    
    # Uwzględnienie kodu pogodowego
    weather_code = weather.get("weather_code", 0)
    if weather_code > 70:  # Śnieg
        weather_factor += 0.25
    elif weather_code > 50:  # Deszcz
        weather_factor += 0.1
    
    # Uwzględnienie wiatru
    wind_speed = weather.get("wind_speed", 0)
    if wind_speed > 40:  # Bardzo silny wiatr
        weather_factor += 0.2
    elif wind_speed > 25:  # Silny wiatr
        weather_factor += 0.1
    
    # Finalna liczba kurierów
    total_couriers = base_couriers * weather_factor
    
    # Zaokrąglenie w górę do najbliższej liczby całkowitej
    return max(1, round(total_couriers))

def calculate_delivery_fee(orders):
    """
    Obliczanie optymalnej opłaty za dostawę
    
    Założenia:
    - Bazowa opłata to 9.99 PLN
    - Złe warunki pogodowe zwiększają opłatę
    - Duża liczba zamówień pozwala obniżyć opłatę (efektywność)
    
    Zwraca: Sugerowaną opłatę za dostawę w PLN
    """
    if not orders:
        return 9.99  # Domyślna opłata
    
    # Sprawdzenie warunków pogodowych (z ostatniego zamówienia)
    last_order = orders[-1]
    weather = last_order.get("weather_data", {})
    
    # Bazowa opłata
    base_fee = 9.99
    
    # Modyfikatory ze względu na pogodę
    weather_factor = 1.0
    
    # Uwzględnienie temperatury
    temperature = weather.get("temperature", 20)
    if temperature < 0:  # Mróz
        weather_factor += 0.2
    elif temperature > 30:  # Upał
        weather_factor += 0.1
    
    # Uwzględnienie opadów
    precipitation = weather.get("precipitation", 0)
    if precipitation > 5:  # Poważne opady
        weather_factor += 0.3
    elif precipitation > 1:  # Umiarkowane opady
        weather_factor += 0.15
    
    # Uwzględnienie kodu pogodowego
    weather_code = weather.get("weather_code", 0)
    if weather_code > 70:  # Śnieg
        weather_factor += 0.25
    elif weather_code > 50:  # Deszcz
        weather_factor += 0.1
    
    # Uwzględnienie wiatru
    wind_speed = weather.get("wind_speed", 0)
    if wind_speed > 40:  # Bardzo silny wiatr
        weather_factor += 0.2
    elif wind_speed > 25:  # Silny wiatr
        weather_factor += 0.1
    
    # Modyfikator na podstawie liczby zamówień (większy wolumen = niższa opłata)
    volume_factor = max(0.85, 1.0 - (len(orders) / 200))
    
    # Finalna opłata
    total_fee = base_fee * weather_factor * volume_factor
    
    # Zaokrąglenie do 0.99 PLN
    return round(total_fee - 0.01, 0) + 0.99

def get_statistics_from_mongo(collection, city, hours=24):
    """Pobieranie statystyk z MongoDB dla danego miasta"""
    try:
        cutoff_time = datetime.now(pytz.timezone('Europe/Warsaw')) - timedelta(hours=hours)
        
        pipeline = [
            {
                "$match": {
                    "city": city,
                    "timestamp_dt": {"$gte": cutoff_time}
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "total_orders": {"$sum": 1},
                    "total_value": {"$sum": "$value_pln"},
                    "avg_delivery_time": {"$avg": "$estimated_delivery_time_min"},
                    "avg_distance": {"$avg": "$distance_km"}
                }
            }
        ]
        
        result = list(collection.aggregate(pipeline))
        if result:
            return result[0]
        return None
    except Exception as e:
        print(f"Błąd podczas pobierania statystyk: {e}")
        return None

def main():
    """Główna funkcja analizująca zamówienia i generująca rekomendacje"""
    consumer = create_consumer()
    mongo_client, mongo_collection = create_mongo_connection()
    
    # Słowniki do przechowywania danych o zamówieniach
    orders_by_city = defaultdict(list)
    last_analysis_time = datetime.now(pytz.timezone('Europe/Warsaw'))
    
    try:
        for message in consumer:
            order = message.value
            city = order.get("city", "Unknown")
            
            # Dodanie zamówienia do listy dla danego miasta
            orders_by_city[city].append(order)
            
            # Zapisanie do MongoDB jeśli połączenie jest aktywne
            if mongo_collection is not None:
                save_to_mongodb(mongo_collection, order)
            
            # Usuwanie starych zamówień (starszych niż ANALYSIS_WINDOW minut)
            current_time = datetime.now(pytz.timezone('Europe/Warsaw'))
            cutoff_time = (current_time - timedelta(minutes=ANALYSIS_WINDOW)).isoformat()
            
            for c in orders_by_city:
                orders_by_city[c] = [
                    o for o in orders_by_city[c] 
                    if o.get("timestamp", "0") > cutoff_time
                ]
            
            # Przeprowadzanie analizy co ANALYSIS_WINDOW minut
            if (current_time - last_analysis_time).total_seconds() >= ANALYSIS_WINDOW * 60:
                print("\n" + "="*50)
                print(f"ANALIZA ZAMÓWIEŃ - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*50)
                
                for city, orders in orders_by_city.items():
                    if orders:
                        # Obliczenie liczby kurierów
                        couriers = calculate_courier_needs(orders, ANALYSIS_WINDOW)
                        
                        # Obliczenie opłaty za dostawę
                        delivery_fee = calculate_delivery_fee(orders)
                        
                        # Podsumowanie pogody
                        last_order = orders[-1]
                        weather = last_order.get("weather_data", {})
                        
                        print(f"\nMiasto: {city}")
                        print(f"Liczba zamówień (ostatnie {ANALYSIS_WINDOW} minut): {len(orders)}")
                        print(f"Całkowita wartość zamówień: {sum(o.get('value_pln', 0) for o in orders):.2f} PLN")
                        print(f"Pogoda: Temp: {weather.get('temperature')}°C, "
                              f"Opady: {weather.get('precipitation')}mm, "
                              f"Wiatr: {weather.get('wind_speed')}km/h")
                        print(f"REKOMENDACJA - Liczba kurierów: {couriers}")
                        print(f"REKOMENDACJA - Opłata za dostawę: {delivery_fee:.2f} PLN")
                        
                        # Pobierz statystyki z ostatnich 24h z MongoDB
                        if mongo_collection is not None:
                            stats = get_statistics_from_mongo(mongo_collection, city)
                            if stats:
                                print(f"\nStatystyki 24h dla {city}:")
                                print(f"  Całkowita liczba zamówień: {stats['total_orders']}")
                                print(f"  Całkowita wartość: {stats['total_value']:.2f} PLN")
                                print(f"  Średni czas dostawy: {stats['avg_delivery_time']:.1f} min")
                                print(f"  Średnia odległość: {stats['avg_distance']:.1f} km")
                
                # Aktualizacja czasu ostatniej analizy
                last_analysis_time = current_time
    
    except KeyboardInterrupt:
        print("\nZatrzymano analizator zamówień")
    finally:
        if mongo_client:
            mongo_client.close()
        consumer.close()

if __name__ == "__main__":
    main()