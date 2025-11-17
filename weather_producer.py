import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
import os  # For secure API key handling

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Securely get API key
API_KEY = os.getenv('WEATHER_API_KEY', 'fa94a1bcf0454cbe8df104136251110')

# Cities to monitor
CITIES = ["Houston, USA", "New York, USA", "Chicago, USA"] #"Houston, USA", "New York, USA", "Chicago, USA"

def fetch_weather_data(city):
    """Fetch weather data for a city using WeatherAPI"""
    url = "http://api.weatherapi.com/v1/current.json"
    params = {
        'key': API_KEY,
        'q': city,
        'aqi': 'no'
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            current = data['current']
            condition = current['condition']
            
            weather_info = {
                'city': data['location']['name'],
                'temperature': current['temp_c'],
                'feels_like': current['feelslike_c'],
                'humidity': current['humidity'],
                'pressure': current['pressure_mb'],
                'weather': condition['text'],
                'description': condition['text'],
                'wind_speed': current['wind_kph'],
                'timestamp': datetime.now().isoformat()
            }
            return weather_info
        else:
            print(f"Error fetching data for {city}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def send_to_kafka():
    """Send weather data to Kafka with alerts"""
    print("🌤️ Starting Weather Producer with WeatherAPI...")
    print(f"Monitoring cities: {', '.join(CITIES)}")
    print("Fetching data every 30 seconds. Press Ctrl+C to stop.\n")
    
    while True:
        for city in CITIES:
            weather_data = fetch_weather_data(city)
            
            if weather_data:
                # Send to Kafka
                producer.send('weather-data', value=weather_data)
                
                temp = weather_data['temperature']
                desc = weather_data['description']
                print(f"✅ {city}: {temp}°C, {desc}") 
                
                # Temperature alert
                if temp > 40:
                    print(f"⚠️ ALERT: High temperature in {city}! {temp}°C")
                elif temp < 5:
                    print(f"❄️ ALERT: Low temperature in {city}! {temp}°C")
                
                # Rain alert
                if 'rain' in desc.lower():
                    print(f"🌧️ ALERT: Rain expected in {city}!")
            
            time.sleep(1)  
        
        print(f"\n⏳ Waiting 30 seconds before next update...\n")
        time.sleep(30)

if __name__ == "__main__":
    send_to_kafka()
