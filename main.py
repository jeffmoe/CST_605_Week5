import logging
import time
import argparse
import sys
import os
import threading
import schedule
import pandas as pd
from datetime import datetime
from kafka.errors import NoBrokersAvailable
from setup_kafka import KafkaSetup
from producer import WeatherProducer
from consumer_processor import WeatherConsumer
from visualization import WeatherVisualizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherPipeline:
    
    def __init__(self, api_key, bootstrap_servers=None, topic=None, output_dir=None):
        self.api_key = api_key
        self.bootstrap_servers = bootstrap_servers or ['localhost:9092']
        self.topic = topic or 'minneapolis-current-weather'
        self.output_dir = output_dir or 'weather_visualizations'
        self.readings = []
        self.data_lock = threading.Lock()
        self.running = False
        self.kafka_producer = None
        self.kafka_consumer = None
        self.kafka_enabled = False
        self.visualization_count = 0
        os.makedirs(self.output_dir, exist_ok=True)
        
    def setup_kafka(self):
        try:
            kafka_setup = KafkaSetup(self.bootstrap_servers)
            if kafka_setup.create_topic(self.topic):
                self.kafka_producer = WeatherProducer(self.bootstrap_servers, self.topic)
                self.kafka_producer.set_api_key(self.api_key)
                if self.kafka_producer.connect():
                    self.kafka_enabled = True
                    logger.info("Kafka setup successful")
                    return True
            logger.warning("Kafka setup failed, continuing without Kafka")
            return False
        except Exception as e:
            logger.warning(f"Kafka not available: {e}")
            return False
    
    def collect_weather_reading(self):
        try:
            temp_producer = WeatherProducer()
            temp_producer.set_api_key(self.api_key)
            
            reading = temp_producer.fetch_current_weather()
            
            if reading:
                with self.data_lock:
                    reading['collection_time'] = datetime.now().isoformat()
                    reading['reading_id'] = len(self.readings) + 1
                    self.readings.append(reading)
                
                logger.info(f"Reading #{len(self.readings)} collected: {reading['temperature_c']}°C, {reading['weather_description']}")
                if self.kafka_enabled and self.kafka_producer:
                    try:
                        key = datetime.now().strftime('%Y%m%d_%H%M%S')
                        self.kafka_producer.producer.send(self.topic, key=key, value=reading)
                        logger.debug(f"Reading sent to Kafka")
                    except Exception as e:
                        logger.error(f"Failed to send to Kafka: {e}")
                
                return True
            else:
                logger.error("Failed to collect weather reading")
                return False
                
        except Exception as e:
            logger.error(f"Error collecting reading: {e}")
            return False
    
    def process_and_visualize(self):
        self.visualization_count += 1
        
        with self.data_lock:
            if not self.readings:
                logger.warning("No readings collected yet. Skipping visualization.")
                return
            df = pd.DataFrame(self.readings)
            logger.info("=" * 60)
            logger.info(f"VISUALIZATION #{self.visualization_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Total readings collected: {len(self.readings)}")
            logger.info(f"Time range: {df['collection_time'].iloc[0]} to {df['collection_time'].iloc[-1]}")
            logger.info("=" * 60)
        try:
            visualizer = WeatherVisualizer(self.output_dir)
            visualizer.generate_histograms(df, visualization_num=self.visualization_count)
            logger.info(f"Visualization #{self.visualization_count} completed")
        except Exception as e:
            logger.error(f"Error generating visualization: {e}")
    
    def data_collection_job(self):
        logger.debug("Running data collection job")
        self.collect_weather_reading()
    
    def visualization_job(self):
        logger.info("Running scheduled visualization job")
        self.process_and_visualize()
    
    def run_scheduled(self, collect_interval=60, visualize_interval=300):
        self.running = True
        
        logger.info("=" * 60)
        logger.info("STARTING SCHEDULED WEATHER PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Data collection interval: {collect_interval} seconds")
        logger.info(f"Visualization interval: {visualize_interval} seconds")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info("=" * 60)
        self.setup_kafka()
        schedule.every(collect_interval).seconds.do(self.data_collection_job)
        schedule.every(visualize_interval).seconds.do(self.visualization_job)
        logger.info("Running initial data collection...")
        self.collect_weather_reading()
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down scheduler...")
        finally:
            self.cleanup()
    
    def run_once(self, num_readings=10, interval=60):
        logger.info("=" * 60)
        logger.info("RUNNING SINGLE BATCH COLLECTION")
        logger.info("=" * 60)
        logger.info(f"Readings to collect: {num_readings}")
        logger.info(f"Interval between readings: {interval} seconds")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info("=" * 60)
        self.setup_kafka()
        for i in range(num_readings):
            logger.info(f"\nReading {i+1}/{num_readings}")
            self.collect_weather_reading()
            
            if i < num_readings - 1:
                logger.info(f"Waiting {interval} seconds...")
                time.sleep(interval)
        logger.info("\nGenerating final visualization...")
        self.process_and_visualize()
        
        self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up...")
        
        if self.kafka_producer:
            self.kafka_producer.close()
        
        if self.kafka_consumer:
            self.kafka_consumer.close()
        
        self.running = False
        logger.info("Cleanup complete")

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Minneapolis Current Weather Pipeline with Scheduled Processing'
    )
    parser.add_argument(
        '--api-key', 
        type=str, 
        required=True,
        help='Your Weatherstack API key (required)'
    )
    parser.add_argument(
        '--mode', 
        type=str, 
        choices=['scheduled', 'once'],
        default='scheduled',
        help='Run mode: scheduled (continuous) or once (single batch)'
    )
    parser.add_argument(
        '--collect-interval', 
        type=int, 
        default=60,
        help='Seconds between data collection (scheduled mode, default: 60)'
    )
    parser.add_argument(
        '--visualize-interval', 
        type=int, 
        default=300,
        help='Seconds between visualizations (scheduled mode, default: 300)'
    )
    parser.add_argument(
        '--num-readings', 
        type=int, 
        default=10,
        help='Number of readings to collect (once mode, default: 10)'
    )
    parser.add_argument(
        '--reading-interval', 
        type=int, 
        default=60,
        help='Seconds between readings (once mode, default: 60)'
    )
    parser.add_argument(
        '--bootstrap-servers', 
        type=str, 
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic', 
        type=str, 
        default='minneapolis-current-weather',
        help='Kafka topic name (default: minneapolis-current-weather)'
    )
    parser.add_argument(
        '--output-dir', 
        type=str, 
        default='weather_visualizations',
        help='Output directory for visualizations (default: weather_visualizations)'
    )
    parser.add_argument(
        '--skip-kafka', 
        action='store_true',
        help='Skip Kafka operations'
    )
    return parser.parse_args()

def main():
    args = parse_arguments()
    bootstrap_servers = [args.bootstrap_servers] if not args.skip_kafka else None
    pipeline = WeatherPipeline(
        api_key=args.api_key,
        bootstrap_servers=bootstrap_servers,
        topic=args.topic if not args.skip_kafka else None,
        output_dir=args.output_dir
    )
    
    try:
        if args.mode == 'scheduled':
            pipeline.run_scheduled(
                collect_interval=args.collect_interval,
                visualize_interval=args.visualize_interval
            )
        else:
            pipeline.run_once(
                num_readings=args.num_readings,
                interval=args.reading_interval
            )
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    finally:
        pipeline.cleanup()

if __name__ == "__main__":
    main()