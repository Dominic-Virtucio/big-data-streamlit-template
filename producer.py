"""
Weather Data Kafka Producer - Streaming Weather Data to Kafka
Fetches real-time weather data from WeatherAPI and streams to Kafka
"""

import argparse
import json
import time
import requests
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


class WeatherDataProducer:
    """
    Kafka producer that fetches and streams real-time weather data
    """
    
    def __init__(self, bootstrap_servers: str, topic: str, api_key: str, locations: list):
        """
        Initialize Weather Data Producer
        
        Parameters:
        - bootstrap_servers: Kafka broker addresses
        - topic: Kafka topic to produce messages to
        - api_key: WeatherAPI.com API key
        - locations: List of locations to monitor
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key
        self.locations = locations
        self.base_url = "http://api.weatherapi.com/v1/current.json"
        
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 1
        }
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"✓ Kafka producer initialized for {bootstrap_servers} on topic '{topic}'")
        except NoBrokersAvailable:
            print(f"✗ ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"✗ ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def fetch_weather_data(self, location: str) -> Dict[str, Any]:
        """
        Fetch real-time weather data from WeatherAPI.com
        
        Parameters:
        - location: City name or coordinates
        
        Returns:
        - Dictionary containing weather data
        """
        try:
            params = {
                'key': self.api_key,
                'q': location,
                'aqi': 'yes'  # Include air quality data
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract and structure relevant weather data
            weather_data = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'location': {
                    'name': data['location']['name'],
                    'region': data['location']['region'],
                    'country': data['location']['country'],
                    'lat': data['location']['lat'],
                    'lon': data['location']['lon'],
                    'timezone': data['location']['tz_id']
                },
                'current': {
                    'temp_c': data['current']['temp_c'],
                    'temp_f': data['current']['temp_f'],
                    'condition': data['current']['condition']['text'],
                    'wind_mph': data['current']['wind_mph'],
                    'wind_kph': data['current']['wind_kph'],
                    'wind_degree': data['current']['wind_degree'],
                    'wind_dir': data['current']['wind_dir'],
                    'pressure_mb': data['current']['pressure_mb'],
                    'pressure_in': data['current']['pressure_in'],
                    'precip_mm': data['current']['precip_mm'],
                    'precip_in': data['current']['precip_in'],
                    'humidity': data['current']['humidity'],
                    'cloud': data['current']['cloud'],
                    'feelslike_c': data['current']['feelslike_c'],
                    'feelslike_f': data['current']['feelslike_f'],
                    'visibility_km': data['current']['vis_km'],
                    'visibility_miles': data['current']['vis_miles'],
                    'uv': data['current']['uv'],
                    'gust_mph': data['current']['gust_mph'],
                    'gust_kph': data['current']['gust_kph']
                },
                'air_quality': data['current'].get('air_quality', {}),
                'last_updated': data['current']['last_updated']
            }
            
            return weather_data
            
        except requests.exceptions.RequestException as e:
            print(f"✗ ERROR fetching weather data for {location}: {e}")
            return None
        except (KeyError, ValueError) as e:
            print(f"✗ ERROR parsing weather data for {location}: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Send message to Kafka topic
        
        Parameters:
        - data: Dictionary containing the message data
        
        Returns:
        - bool: True if successful, False otherwise
        """
        if not self.producer:
            print("✗ ERROR: Kafka producer not initialized")
            return False
        
        if not data:
            print("✗ ERROR: No data to send")
            return False
        
        try:
            future = self.producer.send(self.topic, value=data)
            result = future.get(timeout=10)
            
            location_name = data['location']['name']
            temp = data['current']['temp_c']
            condition = data['current']['condition']
            
            print(f"✓ Sent: {location_name} - {temp}°C, {condition}")
            return True
            
        except KafkaError as e:
            print(f"✗ Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"✗ Unexpected error during send: {e}")
            return False

    def produce_stream(self, interval: int = 60, duration: int = None):
        """
        Main streaming loop - fetches and sends weather data continuously
        
        Parameters:
        - interval: Time between fetches in seconds (default: 60)
        - duration: Total runtime in seconds (None for infinite)
        """
        print(f"\n{'='*60}")
        print(f"🌤️  WEATHER DATA PRODUCER STARTED")
        print(f"{'='*60}")
        print(f"Locations: {', '.join(self.locations)}")
        print(f"Update interval: {interval} seconds")
        print(f"Duration: {duration if duration else 'Infinite'}")
        print(f"{'='*60}\n")
        
        start_time = time.time()
        message_count = 0
        cycle_count = 0
        
        try:
            while True:
                # Check duration limit
                if duration and (time.time() - start_time) >= duration:
                    print(f"\n✓ Reached duration limit of {duration} seconds")
                    break
                
                cycle_count += 1
                print(f"\n--- Cycle {cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
                
                # Fetch and send data for each location
                for location in self.locations:
                    weather_data = self.fetch_weather_data(location)
                    
                    if weather_data:
                        success = self.send_message(weather_data)
                        if success:
                            message_count += 1
                    
                    # Small delay between locations to avoid rate limiting
                    time.sleep(1)
                
                print(f"Total messages sent: {message_count}")
                
                # Wait for next cycle
                print(f"Waiting {interval} seconds until next update...")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Producer interrupted by user")
        except Exception as e:
            print(f"\n✗ Streaming error: {e}")
        finally:
            self.close()
            elapsed = time.time() - start_time
            print(f"\n{'='*60}")
            print(f"📊 PRODUCER SUMMARY")
            print(f"{'='*60}")
            print(f"Total messages sent: {message_count}")
            print(f"Total cycles: {cycle_count}")
            print(f"Runtime: {elapsed:.2f} seconds")
            print(f"{'='*60}\n")

    def close(self):
        """Cleanup and close producer"""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("✓ Kafka producer closed successfully")
            except Exception as e:
                print(f"✗ Error closing Kafka producer: {e}")


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Weather Data Kafka Producer',
        formatter_class=argparse.RawDescriptionHelpFormatter
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
        default='weather-data',
        help='Kafka topic (default: weather-data)'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        required=True,
        help='WeatherAPI.com API key (REQUIRED)'
    )
    
    parser.add_argument(
        '--locations',
        type=str,
        nargs='+',
        default=['Manila', 'Quezon City', 'Makati', 'Cebu City', 'Davao City'],
        help='Locations to monitor (default: Philippine cities)'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Update interval in seconds (default: 60)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Run duration in seconds (default: infinite)'
    )
    
    return parser.parse_args()


def main():
    """Main execution"""
    args = parse_arguments()
    
    # Initialize producer
    producer = WeatherDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        api_key=args.api_key,
        locations=args.locations
    )
    
    # Start producing stream
    try:
        producer.produce_stream(
            interval=args.interval,
            duration=args.duration
        )
    except Exception as e:
        print(f"✗ Main execution error: {e}")
    finally:
        print("Producer execution completed")


if __name__ == "__main__":
    main()
