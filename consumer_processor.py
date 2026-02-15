import json
import pandas as pd
import logging
from collections import deque
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherConsumer:    
    def __init__(self, bootstrap_servers=['localhost:9092'], topic_name='minneapolis-current-weather'):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.data_buffer = deque()
        self.consumer = None
        self.consumer_timeout = 10000
        
    def connect(self):
        try:
            self.consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='weather-consumer-current',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                consumer_timeout_ms=self.consumer_timeout,
                max_poll_records=100,
                fetch_max_wait_ms=5000,
                request_timeout_ms=30000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
                api_version_auto_timeout_ms=30000
            )
            logger.info("Kafka consumer connected successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect consumer: {e}")
            return False
    
    def consume_all(self):
        if not self.consumer and not self.connect():
            return
        
        logger.info(f"Consuming messages from topic '{self.topic_name}'...")
        message_count = 0
        
        try:
            start_time = time.time()
            while time.time() - start_time < 30:
                records = self.consumer.poll(timeout_ms=2000, max_records=100)
                
                if not records:
                    if message_count > 0 and time.time() - start_time > 10:
                        break
                    continue
                
                for tp, messages in records.items():
                    for message in messages:
                        data = message.value
                        data['partition'] = message.partition
                        data['offset'] = message.offset
                        data['key'] = message.key
                        
                        self.data_buffer.append(data)
                        message_count += 1
                        
                        if message_count % 10 == 0:
                            logger.debug(f"Consumed {message_count} messages...")
            
            logger.info(f"Consumed {message_count} messages total")
            
        except KafkaError as e:
            logger.error(f"Kafka error while consuming: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while consuming: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Consumer closed")
    
    def get_dataframe(self):
        if not self.data_buffer:
            logger.warning("No data in buffer")
            return pd.DataFrame()
        df = pd.DataFrame(list(self.data_buffer))
        if 'fetch_timestamp' in df.columns:
            df['fetch_timestamp'] = pd.to_datetime(df['fetch_timestamp'])
        if 'observation_datetime' in df.columns:
            df['observation_datetime'] = pd.to_datetime(df['observation_datetime'], errors='coerce')
        logger.info(f"DataFrame created with {len(df)} rows and columns: {list(df.columns)}")
        return df