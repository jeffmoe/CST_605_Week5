import json
import requests
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherProducer:    
    def __init__(self, bootstrap_servers=['localhost:9092'], topic_name='minneapolis-current-weather'):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.producer = None
        self.api_key = None
        self.base_url = "http://api.weatherstack.com/current"
        self.location = "Minneapolis"
        
    def set_api_key(self, api_key):
        self.api_key = api_key
        
    def connect(self):
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda v: str(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=5,
                    request_timeout_ms=30000,
                    api_version_auto_timeout_ms=30000,
                    max_block_ms=60000
                )
                logger.info("Kafka producer connected successfully")
                return True
            except NoBrokersAvailable:
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"No Kafka brokers available. Retry {retry_count}/{max_retries} in 5 seconds...")
                    time.sleep(5)
                else:
                    logger.error("No Kafka brokers available after multiple retries")
                    return False
            except Exception as e:
                logger.error(f"Failed to connect producer: {e}")
                return False
        
        return False
    
    def fetch_current_weather(self):
        if not self.api_key:
            raise ValueError("API key not set")
        
        try:
            params = {
                'access_key': self.api_key,
                'query': self.location,
                'units': 'm'
            }
            
            logger.info(f"Fetching current weather for {self.location}...")
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if 'error' in data:
                error_info = data['error'].get('info', 'Unknown error')
                logger.error(f"API error: {error_info}")
                return None
            
            current = data.get('current', {})
            location = data.get('location', {})
            request = data.get('request', {})
            
            weather_record = {
                'fetch_timestamp': datetime.now().isoformat(),
                'location': self.location,
                'query_location': request.get('query', self.location),
                'observation_time': current.get('observation_time', ''),
                'observation_datetime': f"{location.get('localtime', '')}",
                'temperature_c': current.get('temperature', 0),
                'weather_code': current.get('weather_code', 0),
                'weather_description': current.get('weather_descriptions', [''])[0],
                'wind_speed_kph': current.get('wind_speed', 0),
                'wind_degree': current.get('wind_degree', 0),
                'wind_dir': current.get('wind_dir', ''),
                'pressure_mb': current.get('pressure', 0),
                'precip_mm': current.get('precip', 0),
                'humidity': current.get('humidity', 0),
                'cloud_cover': current.get('cloudcover', 0),
                'feelslike_c': current.get('feelslike', 0),
                'uv_index': current.get('uv_index', 0),
                'visibility_km': current.get('visibility', 0),
                'is_day': current.get('is_day', 'no'),
                'source': 'weatherstack_free_tier'
            }
            
            logger.info(f"Got current weather for {self.location}: "
                       f"{weather_record['temperature_c']}°C, "
                       f"{weather_record['weather_description']}, "
                       f"Humidity: {weather_record['humidity']}%")
            return weather_record
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Invalid API key. Please check your Weatherstack API key.")
            elif e.response.status_code == 404:
                logger.error("API endpoint not found.")
            else:
                logger.error(f"HTTP error: {e}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching weather data")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error fetching weather data")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching weather data: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}")
            return None
    
    def stream_current_weather(self, interval_seconds=60, num_readings=10):
        if not self.api_key:
            logger.error("API key not set")
            return
        
        if not self.producer and not self.connect():
            logger.warning("Kafka producer not connected. Will fetch data but not stream to Kafka.")
        
        logger.info(f"Starting to fetch current weather for {self.location} every {interval_seconds} seconds")
        logger.info(f"Will collect {num_readings} readings")
        
        readings = []
        failed_attempts = 0
        
        for i in range(num_readings):
            logger.info(f"\nReading {i+1}/{num_readings}")
            weather_data = self.fetch_current_weather()
            
            if weather_data:
                readings.append(weather_data)
                
                if self.producer:
                    try:
                        key = datetime.now().strftime('%Y%m%d_%H%M%S')
                        future = self.producer.send(
                            self.topic_name,
                            key=key,
                            value=weather_data
                        )
                        
                        record_metadata = future.get(timeout=10)
                        logger.info(f"Reading {i+1} sent to Kafka partition {record_metadata.partition}")
                        
                    except KafkaError as e:
                        logger.error(f"Kafka error sending data: {e}")
                        failed_attempts += 1
                    except Exception as e:
                        logger.error(f"Unexpected error sending data: {e}")
                        failed_attempts += 1
                else:
                    logger.info(f"Reading {i+1} fetched (Kafka not connected)")
            else:
                failed_attempts += 1
            
            if i < num_readings - 1:
                logger.info(f"Waiting {interval_seconds} seconds before next reading...")
                time.sleep(interval_seconds)
        
        if self.producer:
            self.producer.flush()
        
        logger.info(f"\nCompleted. Successfully fetched: {len(readings)}/{num_readings}, Failed: {failed_attempts}")
        
        return readings
    
    def stream_single_reading(self):
        if not self.api_key:
            logger.error("API key not set")
            return None
        
        if not self.producer and not self.connect():
            logger.warning("Kafka producer not connected. Will fetch data but not stream to Kafka.")
        
        logger.info(f"Fetching current weather for {self.location}")
        weather_data = self.fetch_current_weather()
        
        if weather_data and self.producer:
            try:
                key = datetime.now().strftime('%Y%m%d_%H%M%S')
                future = self.producer.send(
                    self.topic_name,
                    key=key,
                    value=weather_data
                )
                record_metadata = future.get(timeout=10)
                logger.info(f"Data sent to Kafka partition {record_metadata.partition}")
            except Exception as e:
                logger.error(f"Error sending to Kafka: {e}")
        
        if self.producer:
            self.producer.flush()
        
        return weather_data
    
    def close(self):
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=10)
                logger.info("Producer closed")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")