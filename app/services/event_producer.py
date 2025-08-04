import json
import time
import random
import redis
import os
from confluent_kafka import Producer
from datetime import datetime, timezone
from faker import Faker

from app.models.data_models import User, Item, UserEvent
from vectorizer import get_embedding

fake = Faker()

class EventProducer:
    def __init__(self, kafka_topic: str):
        self.kafka_topic = kafka_topic
        producer_conf = {'bootstrap.servers': 'localhost:9092'}
        self.producer = Producer(producer_conf)
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.user_preferences = {}  # New dictionary to hold user preferences
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
        
        items = []
        try:
            with open('data/items.json', 'r') as f:
                item_data = json.load(f)
                for item_dict in item_data:
                    item_dict['created_at'] = datetime.now(timezone.utc)
                    items.append(Item(**item_dict))
            print("Items loaded from data/items.json.")
        except FileNotFoundError:
            print("Error: 'data/items.json' not found. Please run generate_data.py first.")
            exit()
        
        return users, items

    def pre_populate_redis(self):
        """
        Stores the initial user and item data in Redis,
        now including vector embeddings for items.
        """
        users, items = self.generate_initial_data()
        
        print("Pre-populating Redis with initial data...")
        for user in users:
            user_data = user.model_dump()
            user_data['signup_date'] = int(user_data['signup_date'].timestamp())
            user_data['last_login'] = int(user_data['last_login'].timestamp())
            
            # --- GENERATE USER PREFERENCES ---
            categories = list(set([item.category for item in items]))
            num_preferences = random.randint(1, 3)
            self.user_preferences[user.user_id] = random.sample(categories, num_preferences)
            user_data['preferences'] = self.user_preferences[user.user_id]
            print(f"  > User '{user.user_id}' preferences: {self.user_preferences[user.user_id]}")
            
            user_key = f"user:{user.user_id}"
            self.redis_client.json().set(user_key, '$', user_data)

        for item in items:
            item_data = item.model_dump()
            item_data['created_at'] = int(item_data['created_at'].timestamp())
            
            # --- GENERATE AND STORE THE VECTOR EMBEDDING ---
            if item.description:
                item_data['embedding'] = get_embedding(item.description)
            else:
                item_data['embedding'] = get_embedding(f"{item.title} {item.category}")
            
            item_key = f"item:{item.item_id}"
            self.redis_client.json().set(item_key, '$', item_data)
            print(f"  > Created item: {item_key} with embedding")

        print("Pre-population complete.")

    def simulate_events(self):
        """Simulates a stream of user events with a category preference bias."""
        users, items = self.generate_initial_data()
        event_types = ["view", "click"]
        
        while True:
            # Randomly select a user
            user = random.choice(users)
            
            # 80% chance of picking an item from the user's preferences
            if random.random() < 0.8:
                preferred_categories = self.user_preferences.get(user.user_id, [])
                if preferred_categories:
                    # Find all items from the user's preferred categories
                    possible_items = [item for item in items if item.category in preferred_categories]
                    if possible_items:
                        item = random.choice(possible_items)
                    else:
                        item = random.choice(items) # Fallback if no items match
                else:
                    item = random.choice(items) # Fallback if user has no preferences
            else:
                # 20% chance of picking any random item to discover new interests
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
            time.sleep(random.uniform(0.01, 0.1))

if __name__ == '__main__':
    producer = EventProducer(kafka_topic='user_events')
    producer.pre_populate_redis()
    
    print("\nStarting event simulation. Press Ctrl+C to stop.")
    try:
        producer.simulate_events()
    except KeyboardInterrupt:
        print("\nStopping event producer...")