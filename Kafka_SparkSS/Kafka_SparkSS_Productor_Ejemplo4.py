# kafka_producer_weather.py

import json
import requests
from confluent_kafka import Producer

def fetch_weather_data():
    try:
        #api_url = 'https://api.openweathermap.org/data/2.5/weather?lat=4&lon=74&appid=0d56d52a28e22f403f31d8409b9170dd'
        #api_url = 'https://api.openweathermap.org/data/2.5/weather?lat=4&lon=74&appid=c9ecd6cfdcc822f960f46092301e0a22'
        #curl "https://api.openweathermap.org/data/2.5/weather?lat=4&lon=74&appid=c9ecd6cfdcc822f960f46092301e0a22"
        api_url = 'https://api.openweathermap.org/data/2.5/weather?lat=4&lon=74&appid=4ef0c0b2447c85da15abce49baa090b2'

        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] Al obtener datos del clima: {str(e)}")
        return None

def send_to_kafka(data):
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'spark-producer'
    }
    producer = Producer(producer_config)
    try:
        message = json.dumps(data)
        producer.produce('kafka-spark-api', value=message)
        producer.flush()
        print("[OK] Mensaje enviado a Kafka")
    except Exception as e:
        print(f"[ERROR] Al enviar mensaje a Kafka: {str(e)}")

if __name__ == "__main__":
    weather_data = fetch_weather_data()
    if weather_data:
        send_to_kafka(weather_data)
    else:
        print("[WARN] No se enviaron datos porque la API falló.")
