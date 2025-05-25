import json
import random
import time
import numpy as np
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import pytz

# Konfiguracja
KAFKA_SERVER = "broker:9092"
WEATHER_TOPIC = "weather_data"
ORDERS_TOPIC = "food_orders"

# Typy restauracji i parametry
RESTAURANT_TYPES = [
    "Pizza", "Sushi", "Burger", "Chinese", "Thai", 
    "Italian", "Mexican", "Vegan", "Healthy", "Dessert",
    "Kebab", "Coffee"
]

SHOP_TYPES = ["Grocery"]

BREAKFAST_TYPES = ["Coffee", "Vegan", "Healthy"]
DAY_TYPES = ["Pizza", "Sushi", "Chinese", "Thai", "Italian", "Mexican", "Vegan", "Healthy", "Kebab"]
NIGHT_TYPES = ["Pizza", "Burger", "Kebab"]

# Parametry modelu
BASE_ORDER_RATE = 2.0  # Bazowa liczba zamówień na minutę
MAX_DISTANCE = 20.0    # Maksymalna odległość w km

# Współczynniki miejskie
CITY_FACTORS = {
    "Warsaw": {
        "base_factor": 1.0,
        "special_rules": {
            "remote_work": {"hours": (9, 17), "weekdays_only": True, "factor": 1.15},
            "nightlife": {"hours": (22, 2), "weekends_only": True, "factor": 1.20},
            "business_lunch": {"hours": (12, 14), "weekdays_only": True, "factor": 1.10}
        }
    },
    "Krakow": {
        "base_factor": 0.8,
        "special_rules": {
            "students": {"hours": (18, 23), "factor": 1.25},
            "tourists": {"weekends_only": True, "factor": 0.90},
            "smog": {"weather_condition": 50, "factor": 1.10}
        }
    },
    "Wroclaw": {
        "base_factor": 0.8,
        "special_rules": {
            "it_sector": {"hours": (12, 22), "weekdays_only": True, "factor": 1.20},
            "students": {"hours": (18, 23), "factor": 1.15},
            "weekend_boost": {"weekends_only": True, "factor": 1.10}
        }
    },
    "Gdansk": {
        "base_factor": 0.9,
        "special_rules": {
            "seasonality": {"months": (5, 9), "factor": 0.85},
            "locals_weekday": {"months": (5, 9), "weekdays_only": True, "factor": 1.10},
            "wind_factor": {"weather_condition": "wind", "factor": 1.15}
        }
    },
    "Bialystok": {
        "base_factor": 0.5,
        "special_rules": {
            "traditional": {"hours": (21, 24), "factor": 0.80},
            "night_limited": {"hours": (0, 6), "factor": 0.50},
            "weekend_limited": {"weekends_only": True, "factor": 1.05}
        }
    }
}

# Specjalne współczynniki pogodowe
CITY_WEATHER_FACTORS = {
    "Bialystok": {
        "cold_boost": {"temp_threshold": 0, "factor": 1.10}
    },
    "Gdansk": {
        "wind_multiplier": 1.3
    },
    "Krakow": {
        "fog_smog": {"weather_codes": [45, 48], "factor": 1.20}
    }
}

def create_consumer():
    """Utworzenie konsumenta Kafka dla danych pogodowych"""
    return KafkaConsumer(
        WEATHER_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='order_generator',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def create_producer():
    """Utworzenie producenta Kafka dla zamówień"""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def sigmoid(x, center=0, scale=1):
    """Funkcja sigmoidalna do nieliniowych przekształceń"""
    return 1 / (1 + np.exp(-(x - center) / scale))

def calculate_city_factor(city, current_time, weather_data):
    """
    Oblicza współczynnik dla danego miasta uwzględniając wszystkie specjalne reguły
    """
    if city not in CITY_FACTORS:
        return 1.0
    
    city_data = CITY_FACTORS[city]
    base_factor = city_data["base_factor"]
    special_rules = city_data.get("special_rules", {})
    
    total_factor = base_factor
    
    hour = current_time.hour
    weekday = current_time.weekday()
    month = current_time.month
    is_weekend = weekday >= 5
    
    # Przetwarzanie specjalnych reguł
    for rule_name, rule_data in special_rules.items():
        apply_rule = True
        
        # Sprawdzanie warunków czasowych
        if "hours" in rule_data:
            start_hour, end_hour = rule_data["hours"]
            if end_hour < start_hour:  # Przejście przez północ
                if not (hour >= start_hour or hour < end_hour):
                    apply_rule = False
            else:
                if not (start_hour <= hour < end_hour):
                    apply_rule = False
        
        if "weekdays_only" in rule_data and rule_data["weekdays_only"] and is_weekend:
            apply_rule = False
        
        if "weekends_only" in rule_data and rule_data["weekends_only"] and not is_weekend:
            apply_rule = False
        
        if "months" in rule_data:
            start_month, end_month = rule_data["months"]
            if not (start_month <= month <= end_month):
                apply_rule = False
        
        # Sprawdzanie warunków pogodowych
        if "weather_condition" in rule_data:
            condition = rule_data["weather_condition"]
            if isinstance(condition, int):  # Kod pogodowy
                if weather_data.get("weather_code", 0) < condition:
                    apply_rule = False
            elif condition == "wind":
                if weather_data.get("wind_speed_10m", 0) < 20:
                    apply_rule = False
        
        # Aplikowanie współczynnika jeśli warunki spełnione
        if apply_rule:
            total_factor *= rule_data["factor"]
    
    return total_factor

def calculate_city_weather_factor(city, weather_data):
    """
    Oblicza dodatkowe współczynniki pogodowe specyficzne dla miasta
    """
    if city not in CITY_WEATHER_FACTORS:
        return 1.0
    
    city_weather = CITY_WEATHER_FACTORS[city]
    factor = 1.0
    
    temp = weather_data.get("temperature_2m", 20)
    weather_code = weather_data.get("weather_code", 0)
    wind_speed = weather_data.get("wind_speed_10m", 0)
    
    # Białystok - dodatkowy boost za zimno
    if city == "Bialystok" and "cold_boost" in city_weather:
        if temp < city_weather["cold_boost"]["temp_threshold"]:
            factor *= city_weather["cold_boost"]["factor"]
    
    # Gdańsk - większy wpływ wiatru
    if city == "Gdansk" and "wind_multiplier" in city_weather:
        if wind_speed > 0:
            wind_factor = calculate_wind_factor(wind_speed)
            # Zastępujemy standardowy wind_factor wzmocnionym
            factor *= (wind_factor ** city_weather["wind_multiplier"]) / wind_factor
    
    # Kraków - mgła i smog
    if city == "Krakow" and "fog_smog" in city_weather:
        if weather_code in city_weather["fog_smog"]["weather_codes"]:
            factor *= city_weather["fog_smog"]["factor"]
    
    return factor

def calculate_time_factor(current_time, order_type):
    """
    Obliczanie współczynnika dla pory dnia
    """
    hour = current_time.hour
    weekday = current_time.weekday()  # 0=poniedziałek, 6=niedziela
    is_weekend = weekday >= 5
    
    if order_type == "Shopping":
        # Grocery ma dwa peaki: rano (7-10) i wieczorem (17-20)
        if hour < 6 or hour >= 22:
            return 0.0  # Zamknięte w nocy
        elif 7 <= hour < 10:
            peak_factor = 1.8  # Poranny peak (150% wieczornego)
            if is_weekend:
                peak_factor *= 0.8  # Mniejszy peak w weekend rano
        elif 17 <= hour < 20:
            peak_factor = 1.2  # Wieczorny peak
            if is_weekend:
                peak_factor *= 1.3  # Większy peak w weekend wieczorem
        else:
            peak_factor = 0.5  # Poza peakami
            if is_weekend:
                peak_factor *= 1.2  # Ogólnie więcej zamówień w weekend
    else:
        # Restauracje mają różne peaki i ograniczenia
        if 0 <= hour < 6:
            peak_factor = 0.1  # Bardzo mało zamówień w środku nocy
        elif 6 <= hour < 12:
            peak_factor = 0.4  # Niski poziom rano
        elif 12 <= hour < 15:
            peak_factor = 1.2  # Lunch peak
            if is_weekend:
                peak_factor *= 1.3  # Dłuższe lunche w weekend
        elif 15 <= hour < 18:
            peak_factor = 0.7  # Między peakami
        elif 18 <= hour < 21:
            peak_factor = 1.7  # Wieczorny peak (140% luncha)
            if is_weekend:
                peak_factor *= 1.5  # Znacznie więcej zamówień w weekend wieczorem
        elif 21 <= hour < 22:
            peak_factor = 0.9  # Spadek po kolacji
        else:  # 22-24
            peak_factor = 0.3  # Niski poziom późnym wieczorem
    
    return peak_factor

def get_available_restaurant_types(current_time):
    """
    Zwraca dostępne typy restauracji w zależności od pory dnia
    """
    hour = current_time.hour
    
    if 0 <= hour < 6:
        # Tylko nocne opcje
        return NIGHT_TYPES
    elif 6 <= hour < 12:
        # Śniadaniowe opcje dominują
        return BREAKFAST_TYPES + random.sample(DAY_TYPES, 3)  # Dodajemy kilka innych opcji
    else:
        # Normalne opcje
        return DAY_TYPES
    
def calculate_restaurant_type_probability(restaurant_type, current_time, weather_code):
    """
    Oblicza prawdopodobieństwo wyboru danego typu restauracji
    """
    hour = current_time.hour
    weekday = current_time.weekday()
    is_weekend = weekday >= 5
    
    # Domyślne prawdopodobieństwo
    probability = 1.0 / len(get_available_restaurant_types(current_time))
    
    # Modyfikacje na podstawie pory dnia
    if 6 <= hour < 12:
        # Rano preferujemy śniadaniowe opcje
        if restaurant_type in ["Coffee", "Vegan", "Healthy"]:
            probability *= 3.0  # 70% szans na te opcje
    
    # Modyfikacje na podstawie pogody
    if weather_code > 50:  # Zła pogoda
        if restaurant_type in ["Pizza", "Burger", "Chinese", "Thai"]:
            probability *= 1.5  # Comfort food jest bardziej popularne
    
    # Modyfikacje weekendowe
    if is_weekend and 18 <= hour < 22:
        if restaurant_type in ["Pizza", "Burger", "Chinese", "Kebab"]:
            probability *= 2.0  # Jeszcze więcej comfort food w weekend wieczorem
    
    return probability

def calculate_temperature_factor(temp, hour=None):
    """
    Nieliniowy wpływ temperatury na liczbę zamówień
    Uwzględnia porę dnia i normalne wahania temperatury
    """
    # Jeśli podano godzinę, skoryguj temperaturę względem normalnej
    if hour is not None:
        # Zakładamy średnią dobową amplitudę temperatury około 10°C
        # Najcieplej około 14:00, najzimniej około 4:00
        hour_offset = -5 * np.cos((hour - 14) * np.pi / 12)
        expected_temp = temp + hour_offset
        
        # Używamy różnicy między faktyczną a oczekiwaną temperaturą
        temp_deviation = temp - expected_temp
    else:
        temp_deviation = 0
    
    # Bazowy wpływ temperatury
    if 22 <= temp <= 26:
        base_factor = 1.0
    elif temp < 22:
        # Im zimniej, tym więcej zamówień (max 2.0x przy -5°C)
        cold_factor = 1.0 + sigmoid(temp, center=8, scale=5) * 1.0
        base_factor = min(2.0, cold_factor)
    else:
        # Im cieplej, tym więcej zamówień (max 1.8x przy 35°C)
        heat_factor = 1.0 + sigmoid(temp, center=32, scale=4) * 0.8
        base_factor = min(1.8, heat_factor)
    
    # Dodatkowy wpływ odchylenia od normy
    if temp_deviation < -5:  # Znacznie zimniej niż normalnie o tej porze
        deviation_factor = 1.3
    elif temp_deviation > 5:  # Znacznie cieplej niż normalnie o tej porze
        deviation_factor = 1.2
    else:
        deviation_factor = 1.0
    
    return base_factor * deviation_factor

def calculate_precipitation_factor(rain, snow):
    """
    Nieliniowy wpływ opadów na liczbę zamówień
    Śnieg ma większy wpływ niż deszcz
    """
    rain_factor = 1.0 + (1 - np.exp(-rain * 0.3))  # Asymptotyczny wzrost
    snow_factor = 1.0 + (1 - np.exp(-snow * 0.5))  # Śnieg ma silniejszy wpływ
    
    # Maksymalny wpływ opadów to 3.0x
    return min(3.0, max(rain_factor, snow_factor))

def calculate_wind_factor(wind_speed):
    """
    Nieliniowy wpływ wiatru na liczbę zamówień
    Wykorzystujemy funkcję kwadratową z ograniczeniem
    """
    if wind_speed < 10:
        return 1.0
    elif wind_speed < 25:
        return 1.0 + ((wind_speed - 10) / 15) * 0.3  # Liniowy wzrost do 1.3x
    else:
        # Nieliniowy wzrost dla silnych wiatrów
        factor = 1.3 + (1 - np.exp(-(wind_speed - 25) * 0.05)) * 0.7
        return min(2.0, factor)  # Max 2.0x przy bardzo silnym wietrze

def calculate_weather_code_factor(weather_code):
    """
    Wpływ ogólnego kodu pogodowego na liczbę zamówień
    """
    if weather_code < 50:  # Dobra pogoda
        return 1.0
    elif 50 <= weather_code < 70:  # Opady
        return 1.2
    elif 70 <= weather_code < 80:  # Śnieg
        return 1.5
    elif 80 <= weather_code < 90:  # Deszcz ze śniegiem
        return 1.7
    else:  # Burze (90+)
        return 2.0

def calculate_synergy_effects(weather_data):
    """
    Obliczanie efektów synergicznych między parametrami pogodowymi
    """
    temp = weather_data.get("temperature_2m", 20)
    rain = weather_data.get("rain", 0)
    snow = weather_data.get("snowfall", 0)
    wind_speed = weather_data.get("wind_speed_10m", 0)
    
    synergy_factor = 1.0
    
    # Deszcz + wiatr = większy efekt
    if rain > 0 and wind_speed > 20:
        synergy_factor *= 1.2
    
    # Mróz + śnieg = znacznie większy efekt
    if temp < 0 and snow > 0:
        synergy_factor *= 1.3
    
    # Upał + wysoka wilgotność = większy efekt
    humidity = weather_data.get("relative_humidity_2m", 50)
    if temp > 30 and humidity > 70:
        synergy_factor *= 1.15
    
    return synergy_factor

def calculate_order_rate(weather_data, order_type="Food", city="Warsaw"):
    """
    Obliczanie liczby zamówień na podstawie danych pogodowych, czasu i miasta
    z uwzględnieniem nieliniowych zależności i synergi
    """
    # Pobierz dane pogodowe
    temp = weather_data.get("temperature_2m", 20)
    rain = weather_data.get("rain", 0)
    snow = weather_data.get("snowfall", 0)
    wind_speed = weather_data.get("wind_speed_10m", 0)
    weather_code = weather_data.get("weather_code", 0)
    
    # Pobierz aktualny czas
    current_time = datetime.now()
    hour = current_time.hour
    
    # Oblicz poszczególne współczynniki
    temp_factor = calculate_temperature_factor(temp, hour)
    precip_factor = calculate_precipitation_factor(rain, snow)
    wind_factor = calculate_wind_factor(wind_speed)
    weather_factor = calculate_weather_code_factor(weather_code)
    synergy_factor = calculate_synergy_effects(weather_data)
    
    # Współczynnik czasowy (niezależny od temperatury)
    time_factor = calculate_time_factor(current_time, order_type)
    
    # Współczynniki miejskie
    city_factor = calculate_city_factor(city, current_time, weather_data)
    city_weather_factor = calculate_city_weather_factor(city, weather_data)
    
    # Połącz współczynniki pogodowe
    weather_total_factor = (temp_factor * precip_factor * wind_factor * 
                           weather_factor * synergy_factor * city_weather_factor)
    
    # Zbalansowane połączenie wpływu pogody i czasu
    # Time factor ma większą wagę, aby zachować naturalne wzorce zakupowe
    base_factor = (0.3 * weather_total_factor + 0.7 * time_factor) * \
                  (weather_total_factor ** 0.3)  # Dodatkowy boost przy ekstremalnej pogodzie
    
    # Zastosuj współczynnik miejski
    total_factor = base_factor * city_factor
    
    # Dodaj element losowości (±10%)
    random_factor = 1.0 + random.uniform(-0.1, 0.1)
    total_factor *= random_factor
    
    # Zastosuj ograniczenia (min 0.0x, max 7.0x)
    total_factor = max(0.0, min(7.0, total_factor))
    
    # Oblicz finalną liczbę zamówień na minutę
    return BASE_ORDER_RATE * total_factor

def generate_order(city, weather_data):
    """Generowanie jednego zamówienia z uwzględnieniem wpływu pogody i czasu"""
    current_time = datetime.now(pytz.timezone('Europe/Warsaw'))
    hour = current_time.hour
    weekday = current_time.weekday()
    is_weekend = weekday >= 5
    weather_code = weather_data.get("weather_code", 0)
    
    # Sprawdź czy możemy generować zamówienia o tej porze
    # Zakupy są niedostępne od 22 do 6
    can_order_grocery = not (hour >= 22 or hour < 6)
    
    # Proporcje zamówień zależą od pory i pogody
    if weather_code > 50:  # Zła pogoda
        food_probability = 0.95 if can_order_grocery else 1.0
    else:
        food_probability = 0.85 if can_order_grocery else 1.0
    
    is_food = random.random() < food_probability
    
    if is_food:
        order_type = "Food"
        
        # Pobierz dostępne typy restauracji dla danej pory
        available_types = get_available_restaurant_types(current_time)
        
        # Wybierz typ restauracji z uwzględnieniem prawdopodobieństw
        probabilities = []
        for rest_type in available_types:
            probabilities.append(calculate_restaurant_type_probability(rest_type, current_time, weather_code))
        
        # Normalizuj prawdopodobieństwa
        total_prob = sum(probabilities)
        probabilities = [p/total_prob for p in probabilities]
        
        # Wybierz typ restauracji
        place_type = np.random.choice(available_types, p=probabilities)
    else:
        order_type = "Shopping"
        place_type = "Grocery"
    
    # Generowanie odległości - przy złej pogodzie ludzie zamawiają bliżej
    weather_factor = calculate_weather_code_factor(weather_code)
    
    # Weekend - ludzie skłonni zamawiać z dalszej odległości
    if is_weekend:
        mean_distance = 4.0 / weather_factor
    else:
        mean_distance = 3.0 / weather_factor
    
    distance = random.expovariate(1.0 / mean_distance)
    distance = min(distance, MAX_DISTANCE)
    
    # Generowanie wartości zamówienia
    if is_food:
        base_value = 35.0
        
        # Zamówienia weekendowe są większe
        if is_weekend:
            base_value *= 1.3
        
        # Wieczorne zamówienia są większe
        if 18 <= hour < 22:
            base_value *= 1.2
        
        # Przy złej pogodzie większe zamówienia
        base_value *= weather_factor
        
        value = max(15.0, np.random.normal(base_value, 15.0))
    else:
        base_value = 60.0
        
        # Weekendowe zakupy są większe
        if is_weekend:
            base_value *= 1.4
        
        # Poranne zakupy są nieco większe niż wieczorne
        if 7 <= hour < 10:
            base_value *= 1.1
        
        # Przy złej pogodzie większe zamówienia
        base_value *= weather_factor
        
        value = max(20.0, np.random.normal(base_value, 25.0))
    
    # Obliczanie czasu dostawy
    base_delivery_time = 15 + distance * 5  # 15 min + 5 min/km
    delivery_time_factor = 1.0
    
    # Wpływ pogody na czas dostawy
    temp = weather_data.get("temperature_2m", 20)
    precipitation = weather_data.get("precipitation", 0)
    wind_speed = weather_data.get("wind_speed_10m", 0)
    
    if temp < 0 or temp > 35:
        delivery_time_factor *= 1.2
    if precipitation > 0:
        delivery_time_factor *= 1.0 + min(0.5, precipitation * 0.1)
    if wind_speed > 30:
        delivery_time_factor *= 1.3
    
    # Godziny szczytu wydłużają czas dostawy
    if (12 <= hour < 15) or (18 <= hour < 21):
        delivery_time_factor *= 1.4
    
    delivery_time = round(base_delivery_time * delivery_time_factor)
    
    return {
        "order_id": f"{int(time.time())}-{random.randint(1000, 9999)}",
        "timestamp": current_time.isoformat(),
        "city": city,
        "order_type": order_type,
        "place_type": place_type,
        "distance_km": round(distance, 1),
        "value_pln": round(value, 2),
        "estimated_delivery_time_min": delivery_time,
        "is_weekend": is_weekend,
        "hour": hour,
        "weather_data": {
            "temperature": weather_data.get("temperature_2m"),
            "precipitation": weather_data.get("precipitation"),
            "wind_speed": weather_data.get("wind_speed_10m"),
            "weather_code": weather_data.get("weather_code")
        }
    }

def main():
    """Główna funkcja konsumująca dane pogodowe i generująca zamówienia"""
    consumer = create_consumer()
    producer = create_producer()
    
    try:
        # Pobieranie danych pogodowych
        for message in consumer:
            weather_data = message.value
            city = weather_data.get("city", "Warsaw")
            
            # Obliczenie liczby zamówień na minutę dla różnych typów
            food_order_rate = calculate_order_rate(weather_data, "Food", city)
            shopping_order_rate = calculate_order_rate(weather_data, "Shopping", city)
            
            # Generowanie odpowiedniej liczby zamówień
            # Zakładamy, że dane pogodowe przychodzą co 5 minut
            food_orders_to_generate = int(food_order_rate * 5)
            shopping_orders_to_generate = int(shopping_order_rate * 5)
            total_orders = food_orders_to_generate + shopping_orders_to_generate
            
            current_time = datetime.now(pytz.timezone('Europe/Warsaw'))
            print(f"\n{'='*50}")
            print(f"Czas: {current_time.strftime('%Y-%m-%d %H:%M:%S')} ({'Weekend' if current_time.weekday() >= 5 else 'Dzień roboczy'})")
            print(f"Miasto: {city}, Pogoda: {weather_data.get('weather_code')}, "
                  f"Temp: {weather_data.get('temperature_2m')}°C, "
                  f"Opady: {weather_data.get('precipitation')}mm, "
                  f"Wiatr: {weather_data.get('wind_speed_10m')}km/h")
            print(f"Generowanie {total_orders} zamówień "
                  f"(Food: {food_order_rate:.2f}/min, Shopping: {shopping_order_rate:.2f}/min)")
            print(f"{'='*50}\n")
            
            # Mieszamy typy zamówień
            orders_to_generate = []
            orders_to_generate.extend(['Food'] * food_orders_to_generate)
            orders_to_generate.extend(['Shopping'] * shopping_orders_to_generate)
            random.shuffle(orders_to_generate)
            
            for order_type in orders_to_generate:
                order = generate_order(city, weather_data)
                producer.send(ORDERS_TOPIC, value=order)
                print(f"Zamówienie {order['order_id']} - "
                      f"{order['order_type']} ({order['place_type']}) - "
                      f"{order['value_pln']:.2f} PLN - "
                      f"Dostawa: {order['estimated_delivery_time_min']} min")
                
                # Krótka pauza między zamówieniami dla lepszego rozłożenia w czasie
                time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\nZatrzymano generator zamówień")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()