import json
import time
import random
import redis
from confluent_kafka import Producer
from datetime import datetime, timezone

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
        """Generates mock users and items for our simulation."""
        users = [
            User(user_id='user1', username='alice', signup_date=datetime.now(timezone.utc), last_login=datetime.now(timezone.utc)),
            User(user_id='user2', username='bob', signup_date=datetime.now(timezone.utc), last_login=datetime.now(timezone.utc)),
            User(user_id='user3', username='charlie', signup_date=datetime.now(timezone.utc), last_login=datetime.now(timezone.utc)),
        ]
        items = [
            Item(item_id='itemA', title='The Matrix', category='movie', created_at=datetime.now(timezone.utc)),
            Item(item_id='itemB', title='Inception', category='movie', created_at=datetime.now(timezone.utc)),
            Item(item_id='itemC', title='Dune', category='movie', created_at=datetime.now(timezone.utc)),
            Item(item_id='itemD', title='Foundation', category='series', created_at=datetime.now(timezone.utc)),
        ]
        return users, items

    def pre_populate_redis(self):
        """
        Stores the initial user and item data in Redis using RedisJSON.
        """
        users, items = self.generate_initial_data()
        
        print("Pre-populating Redis with initial data...")
        for user in users:
            # The path '$' is for creating the top-level object
            user_key = f"user:{user.user_id}"
            self.redis_client.json().set(user_key, '$', json.loads(user.model_dump_json()))
            print(f"  > Created user: {user_key}")

        for item in items:
            item_key = f"item:{item.item_id}"
            self.redis_client.json().set(item_key, '$', json.loads(item.model_dump_json()))
            print(f"  > Created item: {item_key}")

        print("Pre-population complete.")

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
    print("\nStarting event simulation. Press Ctrl+C to stop.")
    try:
        producer.simulate_events()
    except KeyboardInterrupt:
        print("\nStopping event producer...")