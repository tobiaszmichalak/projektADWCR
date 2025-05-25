import json
import requests
import time
from datetime import datetime
from kafka import KafkaProducer
import pytz

# Konfiguracja
KAFKA_SERVER = "broker:9092"
KAFKA_TOPIC = "weather_data"
CITY_COORDS = {
    "Warsaw": {"lat": 52.23, "lon": 21.01},
    "Gdansk": {"lat": 54.35, "lon": 18.65},
    "Krakow": {"lat": 50.06, "lon": 19.94},
    "Bialystok": {"lat": 53.13, "lon": 23.16},
    "Wroclaw": {"lat": 51.11, "lon": 17.04}
}
POLL_INTERVAL = 300  # Pobieranie danych co 5 minut (300 sekund)

def create_producer():
    """Utworzenie producenta Kafka"""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def get_weather_data(city, lat, lon):
    """Pobieranie aktualnych danych pogodowych z Open-Meteo API"""
    url = f"https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": [
            "temperature_2m", 
            "relative_humidity_2m",
            "precipitation", 
            "rain", 
            "snowfall", 
            "wind_speed_10m",
            "weather_code"
        ],
        "timezone": "auto"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Dodanie dodatkowych informacji
        current_data = data.get("current", {})
        current_data["city"] = city
        current_data["timestamp"] = datetime.now(pytz.timezone('Europe/Warsaw')).isoformat()
        
        return current_data
    except Exception as e:
        print(f"Błąd podczas pobierania danych pogodowych: {e}")
        return None

def main():
    """Główna funkcja pobierająca dane pogodowe i wysyłająca je do Kafki"""
    producer = create_producer()
    
    try:
        while True:
            for city, coords in CITY_COORDS.items():
                weather_data = get_weather_data(city, coords["lat"], coords["lon"])
                
                if weather_data:
                    print(f"Pobrano dane pogodowe dla {city}: {weather_data}")
                    producer.send(KAFKA_TOPIC, value=weather_data)
            
            # Poczekaj przed kolejnym pobraniem danych
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Zatrzymano pobieranie danych pogodowych")
    finally:
        producer.close()

if __name__ == "__main__":
    main()