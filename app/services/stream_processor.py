import json
import redis
from confluent_kafka import Consumer, KafkaException
from datetime import datetime, timezone
import time

# Import our Pydantic data models
from app.models.data_models import UserEvent

class StreamProcessor:
    def __init__(self, kafka_topic: str):
        self.kafka_topic = kafka_topic
        
        consumer_conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'real-time-feature-group',
            'auto.offset.reset': 'earliest'
        }
        
        try:
            self.consumer = Consumer(consumer_conf)
            print("✅ Kafka Consumer initialized successfully.")
        except KafkaException as e:
            print(f"❌ Failed to initialize Kafka Consumer: {e}")
            raise

        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            self.redis_client.ping()
            print("✅ Redis client initialized successfully.")
        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            raise

    def start(self):
        self.consumer.subscribe([self.kafka_topic])
        print(f"Listening for messages on topic: {self.kafka_topic}...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Kafka error: {msg.error()}")
                        break
                
                self.process_message(msg)

        except KeyboardInterrupt:
            print("Shutting down consumer...")
        finally:
            self.consumer.close()
    
    def process_message(self, msg):
        """
        Parses a Kafka message and updates the Redis feature store.
        """
        message_value = msg.value().decode('utf-8')
        
        try:
            event_data = json.loads(message_value)
            user_event = UserEvent(**event_data)
            
            # --- REAL-TIME FEATURE STORE UPDATE LOGIC ---
            
            # 1. Store the event in a Redis Stream for user history
            stream_key = f"user_history:{user_event.user_id}"
            event_fields = {
                "item_id": user_event.item_id,
                "event_type": user_event.event_type,
                "timestamp": user_event.timestamp.isoformat()
            }
            # Use XADD to add the event to the user's history stream
            self.redis_client.xadd(stream_key, event_fields)
            
            # 2. Update the user's profile with the last login timestamp
            user_key = f"user:{user_event.user_id}"
            self.redis_client.json().set(user_key, '$.last_login', user_event.timestamp.isoformat())

            print(f"✅ Processed event from user '{user_event.user_id}': {user_event.event_type} on item '{user_event.item_id}'")

        except json.JSONDecodeError as e:
            print(f"❌ Failed to decode JSON message: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")

# Main execution block
if __name__ == '__main__':
    processor = StreamProcessor(kafka_topic='user_events')
    processor.start()