import json
import time
import random
import redis
from confluent_kafka import Producer
from datetime import datetime, timezone
import os

from app.models.data_models import User, Item, UserEvent

class EventProducer:
    def __init__(self, kafka_topic: str):
        self.kafka_topic = kafka_topic
        producer_conf = {'bootstrap.servers': 'localhost:9092'}
        self.producer = Producer(producer_conf)
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        print("✅ Kafka Producer and Redis client initialized.")

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f"❌ Message delivery failed: {err}")

    def generate_initial_data(self):
        """Generates mock users and loads items from a JSON file."""
        users = [
            User(user_id=f'user{i}', username=f'user{i}', signup_date=datetime.now(timezone.utc), last_login=datetime.now(timezone.utc))
            for i in range(1, 11)
        ]
        
        # Load items from a JSON file
        items = []
        try:
            with open('data/items.json', 'r') as f:
                item_data = json.load(f)
                for item_dict in item_data:
                    # We need to add the 'created_at' field for the Pydantic model
                    item_dict['created_at'] = datetime.now(timezone.utc)
                    items.append(Item(**item_dict))
            print("Items loaded from data/items.json.")
        except FileNotFoundError:
            print("Error: 'data/items.json' not found. Generating default items.")
            items = [
                Item(item_id='itemA', title='The Matrix', category='movie', created_at=datetime.now(timezone.utc)),
                Item(item_id='itemB', title='Inception', category='movie', created_at=datetime.now(timezone.utc)),
                Item(item_id='itemC', title='Dune', category='movie', created_at=datetime.now(timezone.utc)),
            ]
        
        return users, items

    def pre_populate_redis(self):
        """
        Stores the initial user and item data in Redis using RedisJSON.
        """
        users, items = self.generate_initial_data()
        
        print("Pre-populating Redis with initial data...")
        for user in users:
            user_data = user.model_dump()
            user_data['signup_date'] = int(user_data['signup_date'].timestamp())
            user_data['last_login'] = int(user_data['last_login'].timestamp())
            
            user_key = f"user:{user.user_id}"
            self.redis_client.json().set(user_key, '$', user_data)
            print(f"  > Created user: {user_key}")

        for item in items:
            item_data = item.model_dump()
            item_data['created_at'] = int(item_data['created_at'].timestamp())
            
            item_key = f"item:{item.item_id}"
            self.redis_client.json().set(item_key, '$', item_data)
            print(f"  > Created item: {item_key}")

        print("Pre-population complete.")

    # NEW: Hard-coded function for testing collaborative filtering
    def populate_streams_for_test(self):
        print("Populating streams with hard-coded data for collaborative filtering test...")
        # Clear existing streams to ensure a clean state
        all_stream_keys = self.redis_client.keys("user_history:*")
        if all_stream_keys:
            self.redis_client.delete(*all_stream_keys)
        
        # User 1 views 3 specific items
        user1_events = ['itemA', 'itemB', 'itemC']
        for item_id in user1_events:
            event = UserEvent(user_id='user1', item_id=item_id, event_type='view', timestamp=datetime.now(timezone.utc))
            event_dict = event.model_dump()
            event_dict['timestamp'] = int(event_dict['timestamp'].timestamp()) # CORRECTED: Convert to timestamp
            self.redis_client.xadd('user_history:user1', event_dict, maxlen=100)
            
        # User 2 views 2 of the same items, plus one unique item
        user2_events = ['itemB', 'itemC', 'itemJ']
        for item_id in user2_events:
            event = UserEvent(user_id='user2', item_id=item_id, event_type='view', timestamp=datetime.now(timezone.utc))
            event_dict = event.model_dump()
            event_dict['timestamp'] = int(event_dict['timestamp'].timestamp()) # CORRECTED: Convert to timestamp
            self.redis_client.xadd('user_history:user2', event_dict, maxlen=100)
            
        print("Test streams populated. 'user1' and 'user2' are similar, and 'itemJ' should be recommended to 'user1'.")

    def simulate_events(self):
        """Simulates a stream of user events and sends them to Kafka."""
        users, items = self.generate_initial_data()
        event_types = ["view", "click"]
        
        while True:
            user = random.choice(users)
            item = random.choice(items)
            event_type = random.choice(event_types)
            
            event = UserEvent(
                user_id=user.user_id,
                item_id=item.item_id,
                event_type=event_type,
                timestamp=datetime.now(timezone.utc)
            )
            
            event_json = event.model_dump_json()
            self.producer.produce(self.kafka_topic, value=event_json, callback=self.delivery_report)
            
            self.producer.flush()
            time.sleep(random.uniform(0.5, 1.5))

if __name__ == '__main__':
    producer = EventProducer(kafka_topic='user_events')
    producer.pre_populate_redis()
    
    # We will only run this test function for a moment to set the state
    producer.populate_streams_for_test()
    
    print("\nStarting event simulation. Press Ctrl+C to stop.")
    try:
        producer.simulate_events()
    except KeyboardInterrupt:
        print("\nStopping event producer...")